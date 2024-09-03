use std::time::Duration;

use tonic::transport::ClientTlsConfig;

use crate::members::ServerId;

/// Client config
#[derive(Debug, Clone)]
pub(crate) struct Config {
    /// Local server id, should be initialized on startup
    local_server: Option<ServerId>,
    /// Client tls config
    tls_config: Option<ClientTlsConfig>,
    /// The rpc timeout of a propose request
    propose_timeout: Duration,
    /// The rpc timeout of a 2-RTT request, usually takes longer than propose timeout
    ///
    /// The recommended the values is within (propose_timeout, 2 * propose_timeout].
    wait_synced_timeout: Duration,
}

impl Config {
    /// Get the local server id
    pub(crate) fn local_server(&self) -> Option<ServerId> {
        self.local_server
    }

    /// Get the client TLS config
    pub(crate) fn tls_config(&self) -> Option<&ClientTlsConfig> {
        self.tls_config.as_ref()
    }

    /// Get the propose timeout
    pub(crate) fn propose_timeout(&self) -> Duration {
        self.propose_timeout
    }

    /// Get the wait synced timeout
    pub(crate) fn wait_synced_timeout(&self) -> Duration {
        self.wait_synced_timeout
    }

    /// Returns `true` if the current client is on the server
    pub(crate) fn is_raw_curp(&self) -> bool {
        self.local_server.is_some()
    }
}
