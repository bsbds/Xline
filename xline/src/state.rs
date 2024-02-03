use std::{sync::Arc, time::Duration};

use curp::role_change::RoleChange;

use crate::storage::{compact::Compactor, LeaseStore};

/// State of current node
#[derive(Debug)]
pub(crate) struct State {
    /// lease storage
    lease_storage: Arc<LeaseStore>,
    /// auto compactor
    auto_compactor: Option<Arc<dyn Compactor>>,
}

impl RoleChange for State {
    fn on_election_win(&self) {
        self.lease_storage.promote(Duration::from_secs(1)); // TODO: extend should be election timeout
        if let Some(auto_compactor) = self.auto_compactor.as_ref() {
            auto_compactor.resume();
        }
    }

    fn on_calibrate(&self) {
        self.lease_storage.demote();
        if let Some(auto_compactor) = self.auto_compactor.as_ref() {
            auto_compactor.pause();
        }
    }
}

impl State {
    /// Create a new State
    pub(super) fn new(
        lease_storage: Arc<LeaseStore>,
        auto_compactor: Option<Arc<dyn Compactor>>,
    ) -> Self {
        Self {
            lease_storage,
            auto_compactor,
        }
    }
}
