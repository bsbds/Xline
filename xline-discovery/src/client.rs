use thiserror::Error;
use xline_client::{
    error::ClientError as XlineClientError,
    types::{
        kv::{DeleteRangeRequest, PutRequest, RangeRequest, TxnRequest, Compare, CompareTarget},
        watch::WatchRequest,
    },
    Client, ClientOptions,
};

use crate::{
    host::{Host, HostUrl},
    service::{Service, ServiceUrl},
};

pub(crate) struct XlineClient {
    inner: Client,
}

impl XlineClient {
    async fn connect<E, S>(all_members: S) -> Result<Self, ClientError>
    where
        E: AsRef<str>,
        S: IntoIterator<Item = (E, E)>,
    {
        let client = Client::connect(all_members, ClientOptions::default()).await?;
        Ok(Self { inner: client })
    }

    pub(crate) async fn get_service(&self, url: ServiceUrl) -> Result<Service, ClientError> {
        let req = RangeRequest::new(url);
        let resp = self.inner.kv_client().range(req).await?;
        resp.kvs
            .iter()
            .next()
            .map(|kv| bincode::deserialize::<Service>(&kv.value).map_err(Into::into))
            .ok_or_else(|| ClientError::ServiceNotExist)?
    }

    pub(crate) async fn put_service(
        &self,
        url: ServiceUrl,
        service: Service,
    ) -> Result<(), ClientError> {
        let req = TxnRequest::new().when(vec![Compare::new(url, CompareTarget::Version)])
        let req = PutRequest::new(url, bincode::serialize(&service)?);
        let _resp = self.inner.kv_client().put(req).await?;
        Ok(())
    }

    /// Get hosts from the store
    pub(crate) async fn get_host(&self, url: HostUrl) -> Result<Vec<Host>, ClientError> {
        let req = RangeRequest::new(url);
        let resp = self.inner.kv_client().range(req).await?;
        resp.kvs
            .iter()
            .map(|kv| bincode::deserialize::<Host>(&kv.value))
            .collect::<Result<_, _>>()
            .map_err(Into::into)
    }

    /// Put the host into the store
    pub(crate) async fn put_host(&self, url: HostUrl, host: Host) -> Result<(), ClientError> {
        let req = PutRequest::new(url, bincode::serialize(&host)?);
        let _resp = self.inner.kv_client().put(req).await?;
        Ok(())
    }

    /// Delete hosts from the store
    pub(crate) async fn delete_host(&self, url: String) -> Result<(), ClientError> {
        let req = DeleteRangeRequest::new(url);
        let _resp = self.inner.kv_client().delete(req).await?;
        Ok(())
    }

    /// Watch hosts with a given url
    pub(crate) async fn watch_host(&self, url: String) -> Result<(), ClientError> {
        let req = WatchRequest::new(url);
        let (watcher, stream) = self.inner.watch_client().watch(req).await?;
        Ok(())
    }
}

#[derive(Error, Debug)]
pub(crate) enum ClientError {
    #[error("client propose error: {0}")]
    Propose(String),
    #[error("serialize error: {0}")]
    Serialize(String),
    #[error("service not exist")]
    ServiceNotExist,
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
