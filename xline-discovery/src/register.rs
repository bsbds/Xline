use crate::{
    client::XlineClient,
    host::{Host, HostUrl},
    service::Service,
};

pub struct Registration {
    client: XlineClient,
}

impl Registration {
    async fn register_service(&self, service: Service) {
        self.client.put_service(service.url(), service).await;
    }

    /// Register a host with give service name
    async fn register_host(service: Service, host: Host) {
        let service = Service::new(service_name.clone());
        let host_url = HostUrl::new(service_name, host.id);
    }
}
