use flume::Receiver;
use thiserror::Error;
use xline_client::{
    error::ClientError as XlineClientError,
    types::{
        kv::{
            Compare, CompareResult, CompareTarget, DeleteRangeRequest, PutRequest, RangeRequest,
            TargetUnion, TxnOp, TxnRequest,
        },
        watch::WatchRequest,
    },
    Client, ClientOptions,
};

use crate::{
    host::{Host, HostUrl},
    service::{Service, ServiceInfo, ServiceUrl},
};

const WATCH_CHANNEL_SIZE: usize = 1024;

#[derive(Clone, Debug)]
pub(crate) struct XlineClient {
    inner: Client,
}

impl XlineClient {
    /// Connect to xline cluster
    pub(crate) async fn connect<E, S>(all_members: S) -> Result<Self, ClientError>
    where
        E: AsRef<str>,
        S: IntoIterator<Item = (E, E)>,
    {
        let client = Client::connect(all_members, ClientOptions::default()).await?;
        Ok(Self { inner: client })
    }

    /// Get a service from the store
    pub(crate) async fn get_service(&self, url: ServiceUrl) -> Result<ServiceInfo, ClientError> {
        let req = RangeRequest::new(url);
        let resp = self.inner.kv_client().range(req).await?;
        resp.kvs
            .iter()
            .next()
            .map(|kv| bincode::deserialize::<ServiceInfo>(&kv.value).map_err(Into::into))
            .ok_or_else(|| ClientError::ServiceNotFound)?
    }

    /// Add a service into the store
    pub(crate) async fn add_service(&self, service: Service) -> Result<(), ClientError> {
        let key = service.url();
        let req = TxnRequest::new()
            .when(vec![Compare::new(
                key.clone(),
                CompareResult::Equal,
                CompareTarget::Version,
                TargetUnion::Version(0),
            )])
            .and_then(vec![TxnOp::put(PutRequest::new(
                key,
                bincode::serialize(&service.info())?,
            ))]);
        let resp = self.inner.kv_client().txn(req).await?;

        if !resp.succeeded {
            return Err(ClientError::ServiceExist);
        }

        Ok(())
    }

    /// Get hosts from the store
    pub(crate) async fn get_host(&self, url: HostUrl) -> Result<(i64, Vec<Host>), ClientError> {
        let req = RangeRequest::new(url).with_prefix();
        let resp = self.inner.kv_client().range(req).await?;
        let revision = resp.header.unwrap().revision;
        resp.kvs
            .iter()
            .map(|kv| bincode::deserialize::<Host>(&kv.value))
            .collect::<Result<_, _>>()
            .map_err(Into::into)
            .map(|hosts| (revision, hosts))
    }

    /// Put the host into the store
    pub(crate) async fn add_host(&self, url: HostUrl, host: Host) -> Result<(), ClientError> {
        let req = PutRequest::new(url, bincode::serialize(&host)?);
        let _resp = self.inner.kv_client().put(req).await?;
        Ok(())
    }

    /// Delete hosts from the store
    pub(crate) async fn delete_host(&self, url: HostUrl) -> Result<(), ClientError> {
        let req = DeleteRangeRequest::new(url);
        let _resp = self.inner.kv_client().delete(req).await?;
        Ok(())
    }

    /// Watch hosts with a given url
    pub(crate) async fn watch_host(
        &self,
        url: HostUrl,
        start_revision: i64,
    ) -> Result<Receiver<(EventType, String, Option<Host>)>, ClientError> {
        let (tx, rx) = flume::bounded(WATCH_CHANNEL_SIZE);
        let req = WatchRequest::new(url)
            .with_prefix()
            .with_start_revision(start_revision);
        let (watcher, mut stream) = self.inner.watch_client().watch(req).await?;

        let _handle = tokio::spawn(async move {
            let _watcher = watcher;
            while let Ok(Some(resp)) = stream.message().await {
                for event in resp.events {
                    let Some(kv) = event.kv else { continue };
                    let (event_type, host_id, host) = match event.r#type {
                        0 => (
                            EventType::Put,
                            HostUrl::host_id(&String::from_utf8_lossy(&kv.key)).unwrap_or_default(),
                            Some(bincode::deserialize(&kv.value)?),
                        ),
                        1 => (
                            EventType::Delete,
                            HostUrl::host_id(&String::from_utf8_lossy(&kv.key)).unwrap_or_default(),
                            None,
                        ),
                        _ => unreachable!("event type should only be 0 or 1"),
                    };
                    let _ignore = tx.send((event_type.clone(), host_id, host));
                }
            }

            Ok::<(), ClientError>(())
        });

        Ok(rx)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum EventType {
    /// Put
    Put,
    /// Delete
    Delete,
}

#[derive(Error, Debug)]
pub enum ClientError {
    #[error("client propose error: {0}")]
    Propose(String),
    #[error("serialize error: {0}")]
    Serialize(String),
    #[error("service already exist")]
    ServiceExist,
    #[error("service not exist")]
    ServiceNotFound,
    #[error("no host is in this service")]
    NotHostInService,
}

impl From<XlineClientError> for ClientError {
    fn from(err: XlineClientError) -> Self {
        Self::Propose(err.to_string())
    }
}

impl From<bincode::Error> for ClientError {
    fn from(err: bincode::Error) -> Self {
        Self::Serialize(err.to_string())
    }
}
