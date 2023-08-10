use std::time::Duration;

use log::warn;
use rand::Rng;
use serde::{Deserialize, Serialize};
use tokio::time::interval;
use tokio_stream::StreamExt;
use xline_client::types::lease::LeaseKeeper;

use crate::{
    client::{ClientError, EventType, XlineClient},
    host::{Host, HostUrl},
};

const SERVICE_INFO_PATH: &str = "/service_info";

pub struct ServiceRegistration {
    /// The xline client
    client: XlineClient,
}

impl ServiceRegistration {
    pub async fn connect<E, S>(all_members: S) -> Result<Self, ClientError>
    where
        E: AsRef<str>,
        S: IntoIterator<Item = (E, E)>,
    {
        Ok(Self {
            client: XlineClient::connect(all_members).await?,
        })
    }

    pub async fn register_service(
        &self,
        service_info: ServiceInfo,
    ) -> Result<Service, ClientError> {
        let service = Service {
            client: self.client.clone(),
            info: service_info.clone(),
        };
        if let Err(e) = self.client.add_service(service.clone()).await {
            match e {
                ClientError::ServiceExist => {
                    warn!("service already exist for: {service_info:?}");
                    return Ok(service);
                }
                _ => return Err(e),
            }
        }

        Ok(service)
    }

    pub async fn deregister_service(
        &self,
        service_info: ServiceInfo,
    ) -> Result<Service, ClientError> {
        let service = Service {
            client: self.client.clone(),
            info: service_info.clone(),
        };
        if let Err(e) = self.client.add_service(service.clone()).await {
            match e {
                ClientError::ServiceExist => {
                    warn!("service already exist for: {service_info:?}");
                    return Ok(service);
                }
                _ => return Err(e),
            }
        }

        Ok(service)
    }

    pub async fn get_service(
        &self,
        service_name: impl Into<String>,
    ) -> Result<Service, ClientError> {
        let url = ServiceUrl::new(service_name.into());
        let service_info = self.client.get_service(url).await?;
        Ok(Service {
            client: self.client.clone(),
            info: service_info,
        })
    }
}

/// The discovery service
#[derive(Clone, Debug)]
pub struct Service {
    client: XlineClient,
    info: ServiceInfo,
}

/// Info of the service
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ServiceInfo {
    /// The name of the service
    pub name: String,
}

impl ServiceInfo {
    pub fn new(name: impl Into<String>) -> Self {
        Self { name: name.into() }
    }
}

impl Service {
    /// Register a host
    pub async fn register_host(&self, host: Host) -> Result<(), ClientError> {
        let host_url = HostUrl::new(&self.info.name, host.id);
        self.client.add_host(host_url, host).await
    }

    /// Register a host
    pub async fn register_host_with_keepalive(
        &self,
        host: Host,
    ) -> Result<RegistrationKeeper, ClientError> {
        let host_url = HostUrl::new(&self.info.name, host.id);
        self.client
            .add_host_with_keep_alive(host_url, host)
            .await
            .map(|keeper| RegistrationKeeper(keeper))
    }

    /// Deregister a host
    pub async fn deregister_host(&self, host_id: u64) -> Result<(), ClientError> {
        let host_url = HostUrl::new(&self.info.name, host_id);
        self.client.delete_host(host_url).await
    }

    /// Get info of this service
    pub fn info(&self) -> ServiceInfo {
        self.info.clone()
    }

    /// Get all hosts under this service
    pub async fn all_hosts(&self) -> Result<(i64, Vec<Host>), ClientError> {
        self.client
            .get_host(HostUrl::new_prefix(&self.info.name))
            .await
    }

    /// Get one random hosts under this service
    pub async fn one_host(&self) -> Result<(i64, Host), ClientError> {
        let (revision, hosts) = self
            .client
            .get_host(HostUrl::new_prefix(&self.info.name))
            .await?;

        if hosts.len() == 0 {
            return Err(ClientError::NotHostInService);
        }

        let pos = rand::thread_rng().gen_range(0..hosts.len());

        Ok((
            revision,
            hosts
                .into_iter()
                .skip(pos)
                .next()
                .unwrap_or_else(|| unreachable!("the pos must be in [0, host.len())")),
        ))
    }

    pub async fn subscribe_new(
        &self,
        start_revision: i64,
    ) -> Result<impl StreamExt<Item = Host>, ClientError> {
        Ok(self
            .subscribe_all(start_revision)
            .await?
            .filter_map(|(event, _id, host)| (event == EventType::Put).then(|| host).flatten()))
    }

    pub async fn subscribe_delete(
        &self,
        start_revision: i64,
    ) -> Result<impl StreamExt<Item = u64>, ClientError> {
        Ok(self
            .subscribe_all(start_revision)
            .await?
            .filter_map(|(event, id, _host)| (event == EventType::Delete).then(|| id)))
    }

    pub async fn subscribe_all(
        &self,
        start_revision: i64,
    ) -> Result<impl StreamExt<Item = (EventType, u64, Option<Host>)>, ClientError> {
        let host_event = self
            .client
            .watch_host(HostUrl::new_prefix(&self.info.name), start_revision)
            .await?;

        Ok(host_event.into_stream())
    }

    pub(crate) fn url(&self) -> ServiceUrl {
        ServiceUrl::new(self.info.name.clone())
    }
}

/// The Service URL used in xline store
#[derive(Clone, Debug)]
pub struct ServiceUrl {
    url: String,
}

impl ServiceUrl {
    pub fn new(service_name: String) -> Self {
        Self {
            url: format!("{SERVICE_INFO_PATH}/{service_name}"),
        }
    }
}

impl Into<Vec<u8>> for ServiceUrl {
    fn into(self) -> Vec<u8> {
        self.url.into()
    }
}

#[derive(Debug)]
pub struct RegistrationKeeper(LeaseKeeper);

impl RegistrationKeeper {
    /// Keep alive once
    pub fn keep_alive_once(&mut self) -> Result<(), ClientError> {
        self.0.keep_alive().map_err(Into::into)
    }

    /// Keep alive forever
    pub async fn spawn_keep_alive(mut self, keep_alive_interval: Duration) {
        tokio::spawn(async move {
            let mut interval = interval(keep_alive_interval);
            interval.tick().await;

            while {
                interval.tick().await;
                self.keep_alive_once().is_ok()
            } {}

            Ok::<(), ClientError>(())
        });
    }
}
