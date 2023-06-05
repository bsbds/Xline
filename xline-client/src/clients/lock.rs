use std::sync::Arc;

use clippy_utilities::OverflowArithmetic;
use curp::{client::Client as CurpClient, cmd::ProposeId};
use tonic::transport::Channel;
use uuid::Uuid;
use xline::server::{Command, CommandResponse, KeyRange, SyncResponse};
use xlineapi::{
    Compare, CompareResult, CompareTarget, DeleteRangeRequest, DeleteRangeResponse, EventType,
    LockResponse, PutRequest, RangeRequest, RangeResponse, Request, RequestOp, RequestWithToken,
    RequestWrapper, Response, ResponseHeader, SortOrder, SortTarget, TargetUnion, TxnRequest,
    TxnResponse, UnlockResponse,
};

use crate::{
    clients::{lease::LeaseClient, watch::WatchClient},
    error::{ClientError, Result},
    lease_gen::LeaseIdGenerator,
    types::{
        lease::LeaseGrantRequest,
        lock::{LockRequest, UnlockRequest},
        watch::WatchRequest,
    },
};

/// Client for Lock operations.
#[derive(Clone, Debug)]
pub struct LockClient {
    /// Name of the LockClient
    name: String,
    /// The client running the CURP protocol, communicate with all servers.
    curp_client: Arc<CurpClient<Command>>,
    /// The lease client
    lease_client: LeaseClient,
    /// The watch client
    watch_client: WatchClient,
    /// Auth token
    token: Option<String>,
}

// These methods primarily originate from xline lock server,
// see also: `xline/src/server/lock_server.rs`
impl LockClient {
    /// Creates a new `LockClient`
    #[inline]
    pub fn new(
        name: String,
        curp_client: Arc<CurpClient<Command>>,
        channel: Channel,
        token: Option<String>,
        id_gen: Arc<LeaseIdGenerator>,
    ) -> Self {
        Self {
            name: name.clone(),
            curp_client: Arc::clone(&curp_client),
            lease_client: LeaseClient::new(
                name,
                curp_client,
                channel.clone(),
                token.clone(),
                id_gen,
            ),
            watch_client: WatchClient::new(channel, token.clone()),
            token,
        }
    }

    /// Acquires a distributed shared lock on a given named lock.
    /// On success, it will return a unique key that exists so long as the
    /// lock is held by the caller. This key can be used in conjunction with
    /// transactions to safely ensure updates to etcd only occur while holding
    /// lock ownership. The lock is held until Unlock is called on the key or the
    /// lease associate with the owner expires.
    ///
    /// # Errors
    ///
    /// If `CurpClient` fails to send request
    #[inline]
    pub async fn lock(&mut self, request: LockRequest) -> Result<LockResponse> {
        let mut lease_id = request.inner.lease;
        if lease_id == 0 {
            let resp = self.lease_client.grant(LeaseGrantRequest::new(60)).await?;
            lease_id = resp.id;
        }

        let prefix = format!(
            "{}/",
            String::from_utf8_lossy(&request.inner.name).into_owned()
        );
        let key = format!("{prefix}{lease_id:x}");

        let txn = Self::create_acquire_txn(&prefix, lease_id);
        let (cmd_res, sync_res) = self.propose(txn, false).await?;
        let mut txn_res = Into::<TxnResponse>::into(cmd_res.decode());
        let my_rev = sync_res
            .unwrap_or_else(|| unreachable!("sync_res always has value when use slow path"))
            .revision();
        let owner_res = txn_res
            .responses
            .swap_remove(1)
            .response
            .and_then(|r| {
                if let Response::ResponseRange(res) = r {
                    Some(res)
                } else {
                    None
                }
            })
            .unwrap_or_else(|| unreachable!("owner_resp should be a Get response"));

        let owner_key = owner_res.kvs;
        let header = if owner_key
            .get(0)
            .map_or(false, |kv| kv.create_revision == my_rev)
        {
            owner_res.header
        } else {
            if let Err(e) = self.wait_delete(prefix, my_rev).await {
                let _ignore = self.delete_key(key.as_bytes()).await;
                return Err(e);
            }
            let range_req = RangeRequest {
                key: key.as_bytes().to_vec(),
                ..Default::default()
            };
            let result = self.propose(range_req, true).await;
            match result {
                Ok(res) => {
                    let res = Into::<RangeResponse>::into(res.0.decode());
                    if res.kvs.is_empty() {
                        return Err(ClientError::RpcError(String::from("session expired")));
                    }
                    res.header
                }
                Err(e) => {
                    let _ignore = self.delete_key(key.as_bytes()).await;
                    return Err(e);
                }
            }
        };

        Ok(LockResponse {
            header,
            key: key.into_bytes(),
        })
    }

    /// Takes a key returned by Lock and releases the hold on lock. The
    /// next Lock caller waiting for the lock will then be woken up and given
    /// ownership of the lock.
    ///
    /// # Errors
    ///
    /// If `CurpClient` fails to send request
    #[inline]
    pub async fn unlock(&mut self, request: UnlockRequest) -> Result<UnlockResponse> {
        let header = self.delete_key(&request.inner.key).await?;
        Ok(UnlockResponse { header })
    }

    /// Generate a new `ProposeId`
    fn generate_propose_id(&self) -> ProposeId {
        ProposeId::new(format!("{}-{}", self.name, Uuid::new_v4()))
    }

    /// Generate `Command` proposal from `Request`
    fn command_from_request_wrapper(propose_id: ProposeId, wrapper: RequestWithToken) -> Command {
        #[allow(clippy::wildcard_enum_match_arm)]
        let keys = match wrapper.request {
            RequestWrapper::DeleteRangeRequest(ref req) => {
                vec![KeyRange::new_one_key(req.key.as_slice())]
            }
            RequestWrapper::RangeRequest(ref req) => {
                vec![KeyRange::new(req.key.as_slice(), req.range_end.as_slice())]
            }
            RequestWrapper::TxnRequest(ref req) => req
                .compare
                .iter()
                .map(|cmp| KeyRange::new(cmp.key.as_slice(), cmp.range_end.as_slice()))
                .collect(),
            _ => vec![],
        };
        Command::new(keys, wrapper, propose_id)
    }

    /// Propose request and get result with fast/slow path
    async fn propose<T>(
        &self,
        request: T,
        use_fast_path: bool,
    ) -> Result<(CommandResponse, Option<SyncResponse>)>
    where
        T: Into<RequestWrapper>,
    {
        let request_with_token =
            RequestWithToken::new_with_token(request.into(), self.token.clone());
        let propose_id = self.generate_propose_id();

        let cmd = Self::command_from_request_wrapper(propose_id, request_with_token);

        if use_fast_path {
            let cmd_res = self.curp_client.propose(cmd).await?;
            Ok((cmd_res, None))
        } else {
            let (cmd_res, sync_res) = self.curp_client.propose_indexed(cmd).await?;
            Ok((cmd_res, Some(sync_res)))
        }
    }

    /// Create txn for try acquire lock
    fn create_acquire_txn(prefix: &str, lease_id: i64) -> TxnRequest {
        let key = format!("{prefix}{lease_id:x}");
        #[allow(clippy::as_conversions)] // this cast is always safe
        let cmp = Compare {
            result: CompareResult::Equal as i32,
            target: CompareTarget::Create as i32,
            key: key.as_bytes().to_vec(),
            range_end: vec![],
            target_union: Some(TargetUnion::CreateRevision(0)),
        };
        let put = RequestOp {
            request: Some(Request::RequestPut(PutRequest {
                key: key.as_bytes().to_vec(),
                value: vec![],
                lease: lease_id,
                ..Default::default()
            })),
        };
        let get = RequestOp {
            request: Some(Request::RequestRange(RangeRequest {
                key: key.as_bytes().to_vec(),
                ..Default::default()
            })),
        };
        let range_end = KeyRange::get_prefix(prefix.as_bytes());
        #[allow(clippy::as_conversions)] // this cast is always safe
        let get_owner = RequestOp {
            request: Some(Request::RequestRange(RangeRequest {
                key: prefix.as_bytes().to_vec(),
                range_end,
                sort_order: SortOrder::Ascend as i32,
                sort_target: SortTarget::Create as i32,
                limit: 1,
                ..Default::default()
            })),
        };
        TxnRequest {
            compare: vec![cmp],
            success: vec![put, get_owner.clone()],
            failure: vec![get, get_owner],
        }
    }

    /// Wait until last key deleted
    async fn wait_delete(&self, pfx: String, my_rev: i64) -> Result<()> {
        let rev = my_rev.overflow_sub(1);
        let mut watch_client = self.watch_client.clone();
        loop {
            let range_end = KeyRange::get_prefix(pfx.as_bytes());
            #[allow(clippy::as_conversions)] // this cast is always safe
            let get_req = RangeRequest {
                key: pfx.as_bytes().to_vec(),
                range_end,
                limit: 1,
                sort_order: SortOrder::Descend as i32,
                sort_target: SortTarget::Create as i32,
                max_create_revision: rev,
                ..Default::default()
            };

            let (cmd_res, _sync_res) = self.propose(get_req, false).await?;
            let response = Into::<RangeResponse>::into(cmd_res.decode());
            let last_key = match response.kvs.first() {
                Some(kv) => kv.key.clone(),
                None => return Ok(()),
            };
            let (_, mut response_stream) = watch_client.watch(WatchRequest::new(last_key)).await?;
            while let Some(watch_res) = response_stream.message().await? {
                #[allow(clippy::as_conversions)] // this cast is always safe
                if watch_res
                    .events
                    .iter()
                    .any(|e| e.r#type == EventType::Delete as i32)
                {
                    break;
                }
            }
        }
    }

    /// Delete key
    async fn delete_key(&self, key: &[u8]) -> Result<Option<ResponseHeader>> {
        let del_req = DeleteRangeRequest {
            key: key.into(),
            ..Default::default()
        };
        let (cmd_res, _sync_res) = self.propose(del_req, true).await?;
        let res = Into::<DeleteRangeResponse>::into(cmd_res.decode());
        Ok(res.header)
    }
}
