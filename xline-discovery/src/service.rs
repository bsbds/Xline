use serde::{Deserialize, Serialize};

use crate::{client::XlineClient, host::Host};

const SERVICE_INFO_PATH: &str = "/service_info";

pub struct ServiceRegister {
    /// The xline client
    client: XlineClient,
}

impl ServiceRegister {
    async fn new() -> Self {}

    async fn register_service(&self, service_name: String) -> Service {
        let service = Service {
            name: service_name.clone(),
        };
        self.client
            .put_service(ServiceUrl::new(service_name), service.clone())
            .await;

        service
    }
}

/// The discovery service
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Service {
    /// The name of the service
    name: String,
}

impl Service {
    pub(crate) fn url(&self) -> ServiceUrl {
        ServiceUrl::new(self.name.clone())
    }

    //pub fn all() -> Result<Vec<Host>, Box<dyn std::error::Error>> {}
}

/// The Service URL used in xline store
#[derive(Debug)]
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
