use serde::{Deserialize, Serialize};
use std::collections::HashMap;

const SERVICE_HOST_PATH: &str = "/service";

/// Host store all host's informations
#[derive(Debug, Serialize, Deserialize)]
pub struct Host {
    /// Should be a fully qualified domain name
    pub(crate) hostname: String,
    /// The unique id of this host
    pub(crate) id: String,
    /// The name of the service
    pub(crate) service_name: String,
    /// The protocols this host uses
    /// For examples: ("http", 80), ("ssh", 22), etc.
    pub(crate) protocols: HashMap<String, u16>,
}

impl Host {
    /// get the url of sepcific protocol scheme
    fn url_scheme(&self, scheme: String, path: String) -> Option<String> {
        self.protocols
            .get("scheme")
            .map(|port| format!("{scheme}://{}:{port}{path}", self.hostname))
    }
}

/// The protocol use by host
pub struct Protocol {
    /// The name of the protocol
    name: String,
    /// The port this protocol use
    port: u16,
}

/// The Host URL used in xline store
#[derive(Debug)]
pub struct HostUrl {
    url: String,
}

impl HostUrl {
    pub fn new(service_name: String, host_id: String) -> Self {
        Self {
            url: format!("{SERVICE_HOST_PATH}/{service_name}/{host_id}"),
        }
    }
}

impl Into<Vec<u8>> for HostUrl {
    fn into(self) -> Vec<u8> {
        self.url.into()
    }
}
