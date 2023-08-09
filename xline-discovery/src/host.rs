use serde::{Deserialize, Serialize};
use std::collections::HashMap;

const SERVICE_HOST_PATH: &str = "/service";

/// Host store all host's information
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Host {
    /// The unique id of this host
    pub(crate) id: String,
    /// Should be a fully qualified domain name
    pub(crate) hostname: String,
    /// The name of the service
    pub(crate) service_name: String,
    /// The protocols this host uses
    /// For examples: ("http", 80), ("ssh", 22), etc.
    pub(crate) protocols: HashMap<String, u16>,
}

impl Host {
    /// Creates a new `Host`
    pub fn new<S, P>(id: S, hostname: S, service_name: S, protocols: P) -> Self
    where
        S: Into<String>,
        P: IntoIterator<Item = (S, u16)>,
    {
        Self {
            id: id.into(),
            hostname: hostname.into(),
            service_name: service_name.into(),
            protocols: protocols.into_iter().map(|(s, p)| (s.into(), p)).collect(),
        }
    }
    /// Get the url of specific protocol scheme
    pub fn url_scheme(&self, scheme: String, path: String) -> Option<String> {
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
    pub fn new(service_name: &str, host_id: &str) -> Self {
        Self {
            url: format!("{SERVICE_HOST_PATH}/{service_name}/{host_id}"),
        }
    }

    pub fn new_prefix(service_name: &str) -> Self {
        Self {
            url: format!("{SERVICE_HOST_PATH}/{service_name}"),
        }
    }
}

impl Into<Vec<u8>> for HostUrl {
    fn into(self) -> Vec<u8> {
        self.url.into()
    }
}
