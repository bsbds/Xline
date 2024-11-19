use std::sync::Arc;

use curp_external_api::{
    cmd::{Command, CommandExecutor},
    role_change::RoleChange,
};
use tracing::error;

use crate::{
    rpc::ProposeId,
    server::{StorageApi, DB},
};

use super::CurpNode;

impl<C: Command, CE: CommandExecutor<C>, RC: RoleChange> CurpNode<C, CE, RC> {
    #[allow(clippy::needless_pass_by_value)] // background task needs ownership
    /// A worker responsible for gc the spec pool wal
    pub(crate) fn sp_wal_entry_remove_worker(
        rx: flume::Receiver<Vec<ProposeId>>,
        curp_storage: Arc<DB<C>>,
    ) {
        /// The threshold for removing entries from the WAL.
        const REMOVE_THRESH: usize = 0x1000;
        let mut to_remove = Vec::new();
        while let Ok(ids) = rx.recv() {
            to_remove.extend(ids);
            if to_remove.len() > REMOVE_THRESH {
                if let Err(err) =
                    curp_storage.remove_spec_pool_entries(std::mem::take(&mut to_remove))
                {
                    error!("failed to write remove frame to spec pool WAL: {err}");
                }
            }
        }
    }
}
