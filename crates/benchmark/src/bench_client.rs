use std::fmt::Debug;

use anyhow::Result;
use etcd_client::{Client as EtcdClient, ConnectOptions};
use thiserror::Error;
use xline_client::{
    error::XlineClientError,
    types::kv::{PutRequest, PutResponse, RangeRequest, RangeResponse, TxnRequest, TxnResponse},
    Client, ClientOptions,
};
use xlineapi::command::Command;

/// The client used in benchmark
#[derive(Error, Debug)]
#[non_exhaustive]
pub(crate) enum BenchClientError {
    /// Error from `etcd_client`
    #[error("etcd_client error: {0}")]
    EtcdError(#[from] etcd_client::Error),
    /// Error from `xline_client`
    #[error("xline_client error: {0}")]
    XlineError(#[from] XlineClientError<Command>),
}

/// The KV client enum used in benchmark
pub(crate) enum KVClient {
    /// Xline client
    Xline(Client),
    /// Etcd client
    Etcd(EtcdClient),
}

impl Debug for KVClient {
    #[inline]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match *self {
            KVClient::Xline(ref client) => write!(f, "Xline({client:?})"),
            KVClient::Etcd(ref _client) => write!(f, "Etcd"),
        }
    }
}

/// Benchmark client
pub(crate) struct BenchClient {
    /// Name of the client
    name: String,
    /// KV client instance
    kv_client: KVClient,
}

impl Debug for BenchClient {
    #[inline]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Client")
            .field("name", &self.name)
            .field("kv_client", &self.kv_client)
            .finish()
    }
}

impl BenchClient {
    /// New `Client`
    ///
    /// # Errors
    ///
    /// If `EtcdClient::connect` fails.
    #[inline]
    pub(crate) async fn new(
        addrs: Vec<String>,
        use_curp_client: bool,
        config: ClientOptions,
    ) -> Result<Self> {
        let name = String::from("client");
        let kv_client = if use_curp_client {
            KVClient::Xline(Client::connect(addrs, config).await?)
        } else {
            let options = config
                .tls_config()
                .cloned()
                .map(|c| ConnectOptions::default().with_tls(c));
            KVClient::Etcd(EtcdClient::connect(addrs.clone(), options).await?)
        };
        Ok(Self { name, kv_client })
    }

    /// Send `PutRequest` by `XlineClient` or `EtcdClient`
    ///
    /// # Errors
    ///
    /// If `XlineClient` or `EtcdClient` failed to send request
    #[inline]
    pub(crate) async fn put(
        &mut self,
        request: PutRequest,
    ) -> Result<PutResponse, BenchClientError> {
        match self.kv_client {
            KVClient::Xline(ref mut xline_client) => {
                let response = xline_client.kv_client().put(request).await?;
                Ok(response)
            }
            KVClient::Etcd(ref mut etcd_client) => {
                let opts = convert::put_req(&request);
                let response = etcd_client
                    .put(request.key(), request.value(), Some(opts))
                    .await?;
                Ok(convert::put_res(response))
            }
        }
    }

    /// Send `RangeRequest` by `XlineClient` or `EtcdClient`
    ///
    /// # Errors
    ///
    /// If `XlineClient` or `EtcdClient` failed to send request
    #[inline]
    #[allow(unused)]
    pub(crate) async fn get(
        &mut self,
        request: RangeRequest,
    ) -> Result<RangeResponse, BenchClientError> {
        match self.kv_client {
            KVClient::Xline(ref mut xline_client) => {
                let response = xline_client.kv_client().range(request).await?;
                Ok(response)
            }
            KVClient::Etcd(ref mut etcd_client) => {
                let response = etcd_client.get(request.key(), None).await?;
                Ok(convert::get_res(response))
            }
        }
    }

    /// Send `TxnRequest` by `XlineClient` or `EtcdClient`
    ///
    /// # Errors
    ///
    /// If `XlineClient` or `EtcdClient` failed to send request
    #[inline]
    pub(crate) async fn txn(
        &mut self,
        request: TxnRequest,
    ) -> Result<TxnResponse, BenchClientError> {
        match self.kv_client {
            KVClient::Xline(ref mut xline_client) => {
                let response = xline_client.kv_client().txn(request).await?;
                Ok(response)
            }
            KVClient::Etcd(ref mut etcd_client) => {
                let response = etcd_client.txn(convert::txn_req(request.into())).await?;
                Ok(convert::txn_res(response))
            }
        }
    }
}

/// Convert utils
mod convert {
    use etcd_client::TxnOpResponse;
    use xline_client::types::kv::PutRequest;
    use xlineapi::{KeyValue, PutResponse, RangeResponse, Response, ResponseHeader, ResponseOp};

    /// transform `PutRequest` into `PutOptions`
    pub(super) fn put_req(req: &PutRequest) -> etcd_client::PutOptions {
        let mut opts = etcd_client::PutOptions::new().with_lease(req.lease());
        if req.prev_kv() {
            opts = opts.with_prev_key();
        }
        if req.ignore_value() {
            opts = opts.with_ignore_value();
        }
        if req.ignore_lease() {
            opts = opts.with_ignore_lease();
        }
        opts
    }

    /// transform `etcd_client::PutResponse` into `PutResponse`
    pub(super) fn put_res(res: etcd_client::PutResponse) -> PutResponse {
        let mut res = res;
        PutResponse {
            header: res.take_header().map(|h| ResponseHeader {
                cluster_id: h.cluster_id(),
                member_id: h.member_id(),
                revision: h.revision(),
                raft_term: h.raft_term(),
            }),
            prev_kv: res.take_prev_key().map(|kv| KeyValue {
                key: kv.key().to_vec(),
                create_revision: kv.create_revision(),
                mod_revision: kv.mod_revision(),
                version: kv.version(),
                value: kv.value().to_vec(),
                lease: kv.lease(),
            }),
        }
    }

    /// transform `etcd_client::GetResponse` into `RangeResponse`
    pub(super) fn get_res(res: etcd_client::GetResponse) -> RangeResponse {
        let mut res = res;
        RangeResponse {
            header: res.take_header().map(|h| ResponseHeader {
                cluster_id: h.cluster_id(),
                member_id: h.member_id(),
                revision: h.revision(),
                raft_term: h.raft_term(),
            }),
            kvs: res
                .kvs()
                .iter()
                .map(|kv| KeyValue {
                    key: kv.key().to_vec(),
                    create_revision: kv.create_revision(),
                    mod_revision: kv.mod_revision(),
                    version: kv.version(),
                    value: kv.value().to_vec(),
                    lease: kv.lease(),
                })
                .collect(),
            count: res.count(),
            more: res.more(),
        }
    }

    /// transform `etcd_client::DeleteRangeResponse` into `RangeResponse`
    pub(super) fn delete_res(res: etcd_client::DeleteResponse) -> xlineapi::DeleteRangeResponse {
        let mut res = res;
        xlineapi::DeleteRangeResponse {
            header: res.take_header().map(convert_header),
            deleted: res.deleted(),
            prev_kvs: res
                .prev_kvs()
                .into_iter()
                .cloned()
                .map(convert_key_value)
                .collect(),
        }
    }

    /// transform `TxnRequest` into `Txn`
    pub(super) fn txn_req(txn: xlineapi::TxnRequest) -> etcd_client::Txn {
        etcd_client::Txn::new()
            .when(txn.compare.into_iter().map(convert_cmp).collect::<Vec<_>>())
            .and_then(
                txn.success
                    .into_iter()
                    .map(|t| t.request.unwrap())
                    .map(convert_txn_op)
                    .collect::<Vec<_>>(),
            )
            .or_else(
                txn.failure
                    .into_iter()
                    .map(|t| t.request.unwrap())
                    .map(convert_txn_op)
                    .collect::<Vec<_>>(),
            )
    }

    fn convert_cmp(cmp: xlineapi::Compare) -> etcd_client::Compare {
        let op = match cmp.result {
            0 => etcd_client::CompareOp::Equal,
            1 => etcd_client::CompareOp::Greater,
            2 => etcd_client::CompareOp::Less,
            3 => etcd_client::CompareOp::NotEqual,
            _ => unreachable!(),
        };
        match cmp.target_union.unwrap() {
            xlineapi::TargetUnion::Version(v) => etcd_client::Compare::version(cmp.key, op, v),
            xlineapi::TargetUnion::CreateRevision(c) => {
                etcd_client::Compare::version(cmp.key, op, c)
            }
            xlineapi::TargetUnion::ModRevision(m) => {
                etcd_client::Compare::mod_revision(cmp.key, op, m)
            }
            xlineapi::TargetUnion::Value(v) => etcd_client::Compare::value(cmp.key, op, v),
            xlineapi::TargetUnion::Lease(l) => etcd_client::Compare::lease(cmp.key, op, l),
        }
    }

    // TODO: add options
    fn convert_txn_op(op: xlineapi::Request) -> etcd_client::TxnOp {
        match op {
            xlineapi::Request::RequestRange(r) => etcd_client::TxnOp::get(r.key, None),
            xlineapi::Request::RequestPut(r) => etcd_client::TxnOp::put(r.key, r.value, None),
            xlineapi::Request::RequestDeleteRange(r) => etcd_client::TxnOp::delete(r.key, None),
            xlineapi::Request::RequestTxn(r) => etcd_client::TxnOp::txn(txn_req(r)),
        }
    }

    /// transform `etcd_client::TxnResponse` into `TxnResponse`
    pub(super) fn txn_res(mut res: etcd_client::TxnResponse) -> xlineapi::TxnResponse {
        xlineapi::TxnResponse {
            header: res.take_header().map(convert_header),
            succeeded: res.succeeded(),
            responses: convert_op_response(res.op_responses()),
        }
    }

    fn convert_op_response(op_responses: Vec<TxnOpResponse>) -> Vec<ResponseOp> {
        let mut result = vec![];
        for op in op_responses {
            let xline_op = match op {
                TxnOpResponse::Put(r) => Response::ResponsePut(put_res(r)),
                TxnOpResponse::Get(r) => Response::ResponseRange(get_res(r)),
                TxnOpResponse::Delete(r) => Response::ResponseDeleteRange(delete_res(r)),
                TxnOpResponse::Txn(r) => Response::ResponseTxn(txn_res(r)),
            };
            result.push(ResponseOp {
                response: Some(xline_op),
            });
        }
        result
    }

    fn convert_header(h: etcd_client::ResponseHeader) -> ResponseHeader {
        ResponseHeader {
            cluster_id: h.cluster_id(),
            member_id: h.member_id(),
            revision: h.revision(),
            raft_term: h.raft_term(),
        }
    }

    fn convert_key_value(kv: etcd_client::KeyValue) -> KeyValue {
        KeyValue {
            key: kv.key().to_vec(),
            create_revision: kv.create_revision(),
            mod_revision: kv.mod_revision(),
            version: kv.version(),
            value: kv.value().to_vec(),
            lease: kv.lease(),
        }
    }
}

#[cfg(test)]
#[allow(clippy::panic)]
#[allow(clippy::unwrap_used)]
#[allow(clippy::indexing_slicing)]
mod test {
    use xline_client::types::kv::RangeRequest;
    use xline_test_utils::Cluster;

    use crate::bench_client::{BenchClient, ClientOptions, PutRequest};

    #[tokio::test(flavor = "multi_thread")]
    async fn test_new_xline_client() {
        // create xline client
        let mut cluster = Cluster::new(3).await;
        cluster.start().await;
        let use_curp_client = true;
        let config = ClientOptions::default();
        let mut client = BenchClient::new(cluster.all_client_addrs(), use_curp_client, config)
            .await
            .unwrap();
        //check xline client put value exist
        let request = PutRequest::new("put", "123");
        let _put_response = client.put(request).await;
        let range_request = RangeRequest::new("put");
        let response = client.get(range_request).await.unwrap();
        assert_eq!(response.kvs[0].value, b"123");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_new_etcd_client() {
        let mut cluster = Cluster::new(3).await;
        cluster.start().await;
        let use_curp_client = false;
        let config = ClientOptions::default();
        let mut client = BenchClient::new(cluster.all_client_addrs(), use_curp_client, config)
            .await
            .unwrap();

        let request = PutRequest::new("put", "123");
        let _put_response = client.put(request).await;
        let range_request = RangeRequest::new("put");
        let response = client.get(range_request).await.unwrap();
        assert_eq!(response.kvs[0].value, b"123");
    }
}
