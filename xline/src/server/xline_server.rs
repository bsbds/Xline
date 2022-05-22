use std::{net::SocketAddr, sync::Arc};

use anyhow::Result;
use log::debug;
use prost::Message;
use rpaxos::{
    client::{RpcClient, TcpRpcClient},
    config::Configure,
    server::DefaultServer,
};
use tokio::sync::oneshot;
use tonic::transport::Server;

use crate::rpc::{
    CompactionRequest, CompactionResponse, DeleteRangeRequest, DeleteRangeResponse, Kv, KvServer,
    PutRequest, PutResponse, RangeRequest, RangeResponse, Request, RequestOp, Response, ResponseOp,
    TxnRequest, TxnResponse,
};

use super::command::{Command, CommandExecutor, CommandResponse};

use crate::storage::KvStore;

/// Xline server
#[allow(dead_code)] // Remove this after feature is completed
                    //#[derive(Debug)]
pub(crate) struct XlineServer {
    /// Server name
    name: String,
    /// Address of server
    addr: SocketAddr,
    /// Address of members
    members: Vec<SocketAddr>,
    /// Kv storage
    storage: Arc<Storage>,
    /// Node id
    id: usize,
    /// Consensus Server
    node: Arc<DefaultServer<Command, CommandExecutor>>,
    /// Consensus client
    //client: Arc<TcpRpcClient<Command>>,
    /// Consensus configuration
    config: Configure,
}

/// server storage
#[derive(Debug)]
pub(crate) struct Storage {
    /// KV storage
    storage: KvStore,
}

impl Storage {
    /// Send execution request
    pub(crate) async fn send_req(&self, req: Command) -> oneshot::Receiver<CommandResponse> {
        self.storage.send_req(req).await
    }
}

/// Xline Server Inner
//#[derive(Debug)]
#[allow(dead_code)] // Remove this after feature is completed
struct XlineRpcServer {
    /// KV storage
    storage: Arc<Storage>,
    /// Consensus client
    //client: Arc<TcpRpcClient<Command>>,
    /// Consensus configuration
    config: Configure,
}

impl XlineServer {
    /// New `XlineServer`
    pub(crate) async fn new(
        name: String,
        addr: SocketAddr,
        members: Vec<SocketAddr>,
        id: usize,
    ) -> Self {
        let config = Configure::new(
            members.len(),
            members
                .clone()
                .into_iter()
                .map(|address| address.to_string())
                .collect(),
            id,
            0,
        );
        let storage = Arc::new(Storage {
            storage: KvStore::new(),
        });
        let server = Arc::new(
            DefaultServer::new(config.clone(), CommandExecutor::new(Arc::clone(&storage))).await,
        );
        let server_clone = Arc::clone(&server);
        //let client = Arc::new(TcpRpcClient::<Command>::new(config, 0).await);
        let _handle = tokio::spawn(async move {
            (*server_clone).run().await;
        });
        Self {
            name,
            addr,
            members,
            storage: Arc::new(Storage {
                storage: KvStore::new(),
            }),
            id,
            node: server,
            //client,
            config,
        }
    }

    /// Start `XlineServer`
    pub(crate) async fn start(&self) -> Result<()> {
        let rpc_server = XlineRpcServer {
            storage: Arc::clone(&self.storage),
            //client: Arc::clone(&self.client),
            config: self.config.clone(),
        };
        Ok(Server::builder()
            .add_service(KvServer::new(rpc_server))
            .serve(self.addr)
            .await?)
    }
}

impl XlineRpcServer {
    /// Parse `ResponseOp`
    fn parse_response_op(response_op: ResponseOp) -> Response {
        if let Some(response) = response_op.response {
            response
        } else {
            panic!("Receive empty ResponseOp");
        }
    }
    /// Propose request and get result
    async fn propose(&self, request: Request) -> Vec<Result<CommandResponse, String>> {
        //let range_request = request.into_inner();
        let key = match request {
            Request::RequestRange(ref req) => req.key.clone(),
            Request::RequestPut(ref req) => req.key.clone(),
            Request::RequestDeleteRange(ref req) => req.key.clone(),
            Request::RequestTxn(_) => {
                panic!("Unsupported request");
            }
        };
        let key = std::str::from_utf8(&key)
            .unwrap_or_else(|_| panic!("Failed to convert Vec<u8> to String"))
            .to_owned();
        let range_request_op = RequestOp {
            request: Some(request),
        };
        let cmd = Command::new(key, range_request_op.encode_to_vec());
        let mut client =
            TcpRpcClient::<Command>::new(self.config.clone(), self.config.index()).await;
        client.propose(vec![cmd]).await
        //let result = self.client.propose(vec![cmd]).await;
    }
}

#[tonic::async_trait]
impl Kv for XlineRpcServer {
    /// Range gets the keys in the range from the key-value store.
    async fn range(
        &self,
        request: tonic::Request<RangeRequest>,
    ) -> Result<tonic::Response<RangeResponse>, tonic::Status> {
        debug!("Receive RangeRequest {:?}", request);

        let range_request = request.into_inner();
        let mut result = self.propose(Request::RequestRange(range_request)).await;
        //let result = self.client.propose(vec![cmd]).await;
        match result.swap_remove(0) {
            Ok(res_op) => {
                let res = XlineRpcServer::parse_response_op(res_op.decode());
                if let Response::ResponseRange(response) = res {
                    Ok(tonic::Response::new(response))
                } else {
                    panic!("Receive wrong response {:?}", res);
                }
            }
            Err(e) => panic!(
                "Failed to receive response from KV storage, error is {:?}",
                e
            ),
        }
    }
    /// Put puts the given key into the key-value store.
    /// A put request increments the revision of the key-value store
    /// and generates one event in the event history.
    async fn put(
        &self,
        request: tonic::Request<PutRequest>,
    ) -> Result<tonic::Response<PutResponse>, tonic::Status> {
        debug!("Receive PutRequest {:?}", request);
        let put_request = request.into_inner();
        let mut result = self.propose(Request::RequestPut(put_request)).await;
        match result.swap_remove(0) {
            Ok(res_op) => {
                let res = XlineRpcServer::parse_response_op(res_op.decode());
                if let Response::ResponsePut(response) = res {
                    Ok(tonic::Response::new(response))
                } else {
                    panic!("Receive wrong response {:?}", res);
                }
            }
            Err(e) => panic!(
                "Failed to receive response from KV storage, error is {:?}",
                e
            ),
        }
    }
    /// DeleteRange deletes the given range from the key-value store.
    /// A delete request increments the revision of the key-value store
    /// and generates a delete event in the event history for every deleted key.
    async fn delete_range(
        &self,
        request: tonic::Request<DeleteRangeRequest>,
    ) -> Result<tonic::Response<DeleteRangeResponse>, tonic::Status> {
        debug!("Receive DeleteRangeRequest {:?}", request);
        let delete_range_request = request.into_inner();
        let mut result = self
            .propose(Request::RequestDeleteRange(delete_range_request))
            .await;
        match result.swap_remove(0) {
            Ok(res_op) => {
                let res = XlineRpcServer::parse_response_op(res_op.decode());
                if let Response::ResponseDeleteRange(response) = res {
                    Ok(tonic::Response::new(response))
                } else {
                    panic!("Receive wrong response {:?}", res);
                }
            }
            Err(e) => panic!(
                "Failed to receive response from KV storage, error is {:?}",
                e
            ),
        }
    }
    /// Txn processes multiple requests in a single transaction.
    /// A txn request increments the revision of the key-value store
    /// and generates events with the same revision for every completed request.
    /// It is not allowed to modify the same key several times within one txn.
    async fn txn(
        &self,
        request: tonic::Request<TxnRequest>,
    ) -> Result<tonic::Response<TxnResponse>, tonic::Status> {
        debug!("Receive TxnRequest {:?}", request);
        Err(tonic::Status::new(
            tonic::Code::Unimplemented,
            "Not Implemented".to_owned(),
        ))
    }
    /// Compact compacts the event history in the etcd key-value store. The key-value
    /// store should be periodically compacted or the event history will continue to grow
    /// indefinitely.
    async fn compact(
        &self,
        request: tonic::Request<CompactionRequest>,
    ) -> Result<tonic::Response<CompactionResponse>, tonic::Status> {
        debug!("Receive CompactionRequest {:?}", request);
        Err(tonic::Status::new(
            tonic::Code::Unimplemented,
            "Not Implemented".to_owned(),
        ))
    }
}
