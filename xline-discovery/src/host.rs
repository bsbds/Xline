use serde::{Deserialize, Serialize};
use std::{
    collections::{hash_map::DefaultHasher, HashMap},
    hash::Hasher,
    time::SystemTime,
};

const SERVICE_HOST_PATH: &str = "/service";

/// Host store all host's information
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Host {
    /// The unique id of this host
    pub(crate) id: u64,
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
    pub fn new<H, S, P, PS>(hostname: H, service_name: S, protocols: P) -> Self
    where
        H: AsRef<str>,
        S: AsRef<str>,
        PS: AsRef<str>,
        P: IntoIterator<Item = (PS, u16)>,
    {
        let hostname = hostname.as_ref().to_owned();
        let service_name = service_name.as_ref().to_owned();
        Self {
            id: Self::gen_host_id(hostname.clone(), service_name.clone()),
            hostname,
            service_name,
            protocols: protocols
                .into_iter()
                .map(|(s, p)| (s.as_ref().to_owned(), p))
                .collect(),
        }
    }

    /// Get id
    pub fn id(&self) -> u64 {
        self.id
    }

    /// Get the url of specific protocol scheme
    pub fn url_scheme(&self, scheme: String, path: String) -> Option<String> {
        self.protocols
            .get("scheme")
            .map(|port| format!("{scheme}://{}:{port}{path}", self.hostname))
    }

    fn gen_host_id(hostname: String, service_name: String) -> u64 {
        let ts = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_else(|e| unreachable!("SystemTime before UNIX EPOCH! {e}"))
            .as_secs();

        let mut hasher = DefaultHasher::new();
        hasher.write(service_name.as_bytes());
        hasher.write(hostname.as_bytes());
        hasher.write_u64(ts);
        hasher.finish()
    }
}

/// The protocol use by host
pub struct Protocol {
    /// The name of the protocol
    pub name: String,
    /// The port this protocol use
    pub port: u16,
}

/// The Host URL used in xline store
#[derive(Debug)]
pub struct HostUrl {
    url: String,
}

impl HostUrl {
    pub fn new(service_name: &str, host_id: u64) -> Self {
        Self {
            url: format!("{SERVICE_HOST_PATH}/{service_name}/{host_id}"),
        }
    }

    pub fn new_prefix(service_name: &str) -> Self {
        Self {
            url: format!("{SERVICE_HOST_PATH}/{service_name}"),
        }
    }

    pub fn host_id(url: &str) -> Option<u64> {
        url.split('/').next_back().map(|s| s.parse().ok()).flatten()
    }
}

impl Into<Vec<u8>> for HostUrl {
    fn into(self) -> Vec<u8> {
        self.url.into()
    }
}
