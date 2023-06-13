use std::sync::Arc;

use clippy_utilities::OverflowArithmetic;
use curp::{client::Client as CurpClient, cmd::ProposeId};
use tonic::transport::Channel;
use uuid::Uuid;
use xline::server::{Command, KeyRange};
use xlineapi::{
    Compare, CompareResult, CompareTarget, DeleteRangeRequest, DeleteRangeResponse, EventType,
    LockResponse, PutRequest, RangeRequest, RangeResponse, Request, RequestOp, RequestWithToken,
    Response, ResponseHeader, SortOrder, SortTarget, TargetUnion, TxnRequest, TxnResponse,
    UnlockResponse,
};

use crate::{
    clients::{
        lease::{LeaseClient, LeaseGrantRequest},
        watch::{WatchClient, WatchRequest},
    },
    error::{ClientError, Result},
    lease_gen::LeaseIdGenerator,
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

impl LockClient {
    /// New `LockClient`
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

        let txn_request = Self::create_acquire_txn(&prefix, lease_id);
        let key_ranges = txn_request
            .compare
            .iter()
            .map(|cmp| KeyRange::new(cmp.key.as_slice(), cmp.range_end.as_slice()))
            .collect();

        let (cmd_res, sync_res) = self
            .curp_client
            .propose_indexed(Command::new(
                key_ranges,
                RequestWithToken::new_with_token(txn_request.into(), self.token.clone()),
                self.generate_propose_id(),
            ))
            .await?;
        let mut txn_res = Into::<TxnResponse>::into(cmd_res.decode());

        let my_rev = sync_res.revision();
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
            if let Err(e) = self.wait_delete(prefix, my_rev, None).await {
                let _ignore = self.delete_key(key.as_bytes(), None).await;
                return Err(e);
            }
            let range_req = RangeRequest {
                key: key.as_bytes().to_vec(),
                ..Default::default()
            };
            let result = self
                .curp_client
                .propose(Command::new(
                    vec![KeyRange::new_one_key(key.as_bytes().to_vec())],
                    RequestWithToken::new_with_token(range_req.into(), self.token.clone()),
                    self.generate_propose_id(),
                ))
                .await;
            match result {
                Ok(res) => {
                    let res = Into::<RangeResponse>::into(res.decode());
                    if res.kvs.is_empty() {
                        return Err(ClientError::RpcError(String::from("session expired")));
                    }
                    res.header
                }
                Err(e) => {
                    let _ignore = self.delete_key(key.as_bytes(), None).await;
                    return Err(e.into());
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
        let header = self.delete_key(&request.inner.key, None).await?;
        Ok(UnlockResponse { header })
    }

    /// Generate a new `ProposeId`
    fn generate_propose_id(&self) -> ProposeId {
        ProposeId::new(format!("{}-{}", self.name, Uuid::new_v4()))
    }

    /// Crate txn for try acquire lock
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
    async fn wait_delete(&self, pfx: String, my_rev: i64, _token: Option<&String>) -> Result<()> {
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

            let request = RequestWithToken::new_with_token(get_req.into(), self.token.clone());
            let propose_id = self.generate_propose_id();
            let cmd = Command::new(
                vec![KeyRange::new_one_key(pfx.as_bytes().to_vec())],
                request,
                propose_id,
            );
            let cmd_res = self.curp_client.propose(cmd).await?;

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
    async fn delete_key(
        &self,
        key: &[u8],
        _token: Option<String>,
    ) -> Result<Option<ResponseHeader>> {
        let del_req = DeleteRangeRequest {
            key: key.into(),
            ..Default::default()
        };
        let request = RequestWithToken::new_with_token(del_req.into(), self.token.clone());
        let propose_id = self.generate_propose_id();
        let cmd = Command::new(vec![KeyRange::new_one_key(key)], request, propose_id);
        let cmd_res = self.curp_client.propose(cmd).await?;
        let res = Into::<DeleteRangeResponse>::into(cmd_res.decode());
        Ok(res.header)
    }
}

/// Request for `Lock`
#[derive(Debug, PartialEq)]
pub struct LockRequest {
    /// inner request
    inner: xlineapi::LockRequest,
}

impl LockRequest {
    /// Create a new `LockRequest`
    #[inline]
    #[must_use]
    pub const fn new() -> Self {
        Self {
            inner: xlineapi::LockRequest {
                name: Vec::new(),
                lease: 0,
            },
        }
    }

    /// Set name.
    #[inline]
    #[must_use]
    pub fn with_name(mut self, name: impl Into<Vec<u8>>) -> Self {
        self.inner.name = name.into();
        self
    }

    /// Set lease.
    #[inline]
    #[must_use]
    pub const fn with_lease(mut self, lease: i64) -> Self {
        self.inner.lease = lease;
        self
    }
}

impl From<LockRequest> for xlineapi::LockRequest {
    #[inline]
    fn from(req: LockRequest) -> Self {
        req.inner
    }
}

/// Request for `Unlock`
#[derive(Debug)]
pub struct UnlockRequest {
    /// inner request
    inner: xlineapi::UnlockRequest,
}

impl UnlockRequest {
    /// Create a new `UnlockRequest`
    #[inline]
    #[must_use]
    pub const fn new() -> Self {
        Self {
            inner: xlineapi::UnlockRequest { key: Vec::new() },
        }
    }

    /// Set key.
    #[inline]
    #[must_use]
    pub fn with_key(mut self, key: impl Into<Vec<u8>>) -> Self {
        self.inner.key = key.into();
        self
    }
}

impl From<UnlockRequest> for xlineapi::UnlockRequest {
    #[inline]
    fn from(req: UnlockRequest) -> Self {
        req.inner
    }
}
