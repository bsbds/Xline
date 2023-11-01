use std::sync::Arc;

use curp::{
    cmd::{Command as CurpCommand, CommandExecutor as CurpCommandExecutor},
    error::{CommandProposeError, ProposeError},
    LogIndex,
};
use engine::Snapshot;
use xlineapi::RequestWrapper;

use super::barriers::{IdBarrier, IndexBarrier};
use crate::{
    revision_number::RevisionNumberGenerator,
    rpc::RequestBackend,
    storage::{db::WriteOp, storage_api::StorageApi, AuthStore, KvStore, LeaseStore},
};

pub use xlineapi::command::{Command, CommandResponse, KeyRange, SyncResponse};

/// Meta table name
pub(crate) const META_TABLE: &str = "meta";
/// Key of applied index
pub(crate) const APPLIED_INDEX_KEY: &str = "applied_index";

/// Range start and end to get all keys
const UNBOUNDED: &[u8] = &[0_u8];
/// Range end to get one key
const ONE_KEY: &[u8] = &[];

/// Type of `KeyRange`
pub(crate) enum RangeType {
    /// `KeyRange` contains only one key
    OneKey,
    /// `KeyRange` contains all keys
    AllKeys,
    /// `KeyRange` contains the keys in the range
    Range,
}

impl RangeType {
    /// Get `RangeType` by given `key` and `range_end`
    #[inline]
    pub(crate) fn get_range_type(key: &[u8], range_end: &[u8]) -> Self {
        if range_end == ONE_KEY {
            RangeType::OneKey
        } else if key == UNBOUNDED && range_end == UNBOUNDED {
            RangeType::AllKeys
        } else {
            RangeType::Range
        }
    }
}

/// Command Executor
#[derive(Debug)]
pub(crate) struct CommandExecutor<S>
where
    S: StorageApi,
{
    /// Kv Storage
    kv_storage: Arc<KvStore<S>>,
    /// Auth Storage
    auth_storage: Arc<AuthStore<S>>,
    /// Lease Storage
    lease_storage: Arc<LeaseStore<S>>,
    /// persistent storage
    persistent: Arc<S>,
    /// Barrier for applied index
    index_barrier: Arc<IndexBarrier>,
    /// Barrier for propose id
    id_barrier: Arc<IdBarrier>,
    /// Revision Number generator for KV request and Lease request
    general_rev: Arc<RevisionNumberGenerator>,
    /// Revision Number generator for Auth request
    auth_rev: Arc<RevisionNumberGenerator>,
}

impl<S> CommandExecutor<S>
where
    S: StorageApi,
{
    /// New `CommandExecutor`
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        kv_storage: Arc<KvStore<S>>,
        auth_storage: Arc<AuthStore<S>>,
        lease_storage: Arc<LeaseStore<S>>,
        persistent: Arc<S>,
        index_barrier: Arc<IndexBarrier>,
        id_barrier: Arc<IdBarrier>,
        general_rev: Arc<RevisionNumberGenerator>,
        auth_rev: Arc<RevisionNumberGenerator>,
    ) -> Self {
        Self {
            kv_storage,
            auth_storage,
            lease_storage,
            persistent,
            index_barrier,
            id_barrier,
            general_rev,
            auth_rev,
        }
    }
}

#[async_trait::async_trait]
impl<S> CurpCommandExecutor<Command> for CommandExecutor<S>
where
    S: StorageApi,
{
    fn prepare(
        &self,
        cmd: &Command,
        index: LogIndex,
    ) -> Result<<Command as CurpCommand>::PR, <Command as CurpCommand>::Error> {
        let wrapper = cmd.request();
        if let Err(e) = self.auth_storage.check_permission(wrapper) {
            self.id_barrier.trigger(cmd.id(), -1);
            self.index_barrier.trigger(index, -1);
            return Err(e);
        }
        let revision = match wrapper.request.backend() {
            RequestBackend::Auth => {
                if wrapper.request.skip_auth_revision() {
                    -1
                } else {
                    self.auth_rev.next()
                }
            }
            RequestBackend::Kv => {
                if self.kv_storage.is_read_only(wrapper) {
                    -1
                } else {
                    self.general_rev.next()
                }
            }
            RequestBackend::Lease => {
                if wrapper.request.skip_lease_revision() {
                    -1
                } else {
                    self.general_rev.next()
                }
            }
        };
        Ok(revision)
    }

    fn prepare_commit(
        &self,
        cmd: &Command,
        index: LogIndex,
    ) -> Result<<Command as CurpCommand>::PR, <Command as CurpCommand>::Error> {
        let wrapper = cmd.request();
        if let Err(e) = self.auth_storage.check_permission(wrapper) {
            self.id_barrier.trigger(cmd.id(), -1);
            self.index_barrier.trigger(index, -1);
            return Err(e);
        }
        let revision = match wrapper.request.backend() {
            RequestBackend::Auth => {
                if wrapper.request.skip_auth_revision() {
                    -1
                } else {
                    self.auth_rev.next_commit()
                }
            }
            RequestBackend::Kv => {
                if self.kv_storage.is_read_only(wrapper) {
                    -1
                } else {
                    self.general_rev.next_commit()
                }
            }
            RequestBackend::Lease => {
                if wrapper.request.skip_lease_revision() {
                    -1
                } else {
                    self.general_rev.next_commit()
                }
            }
        };
        Ok(revision)
    }

    fn prepare_reset(&self) {
        let _ignore = self.auth_rev.reset();
        let _ignore = self.general_rev.reset();
    }

    async fn execute(
        &self,
        cmd: &Command,
        index: LogIndex,
        revision: i64,
    ) -> Result<<Command as CurpCommand>::ER, <Command as CurpCommand>::Error> {
        let wrapper = cmd.request();
        let res = match wrapper.request.backend() {
            RequestBackend::Kv => self.kv_storage.execute(wrapper, 0, revision),
            RequestBackend::Auth => self.auth_storage.execute(wrapper),
            RequestBackend::Lease => self.lease_storage.execute(wrapper),
        };
        match res {
            Ok(res) => Ok(res),
            Err(e) => {
                self.id_barrier.trigger(cmd.id(), -1);
                self.index_barrier.trigger(index, -1);
                Err(e)
            }
        }
    }

    async fn after_sync(
        &self,
        cmd: &Command,
        index: LogIndex,
        revision: i64,
    ) -> Result<<Command as CurpCommand>::ASR, <Command as CurpCommand>::Error> {
        let mut ops = vec![WriteOp::PutAppliedIndex(index)];
        let wrapper = cmd.request();

        // Flush the applied index, as we will flush the ops inside the kv storage for txn
        if let RequestWrapper::TxnRequest(_) = wrapper.request {
            let _ignore = self.persistent.flush_ops(ops.split_off(0))?;
        }

        let (res, mut wr_ops) = match wrapper.request.backend() {
            RequestBackend::Kv => self.kv_storage.after_sync(wrapper, revision).await?,
            RequestBackend::Auth => self.auth_storage.after_sync(wrapper, revision)?,
            RequestBackend::Lease => self.lease_storage.after_sync(wrapper, revision).await?,
        };
        tracing::debug!("after sync {cmd:?} with index: {index}, revision: {revision}");
        ops.append(&mut wr_ops);
        let key_revisions = self.persistent.flush_ops(ops)?;
        if !key_revisions.is_empty() {
            self.kv_storage.insert_index(key_revisions);
        }
        self.lease_storage.mark_lease_synced(&wrapper.request);
        self.id_barrier.trigger(cmd.id(), revision);
        self.index_barrier.trigger(index, revision);

        Ok(res)
    }

    async fn reset(
        &self,
        snapshot: Option<(Snapshot, LogIndex)>,
    ) -> Result<(), <Command as CurpCommand>::Error> {
        let s = if let Some((snapshot, index)) = snapshot {
            _ = self
                .persistent
                .flush_ops(vec![WriteOp::PutAppliedIndex(index)])?;
            Some(snapshot)
        } else {
            None
        };
        self.persistent.reset(s).await
    }

    async fn snapshot(&self) -> Result<Snapshot, <Command as CurpCommand>::Error> {
        let path = format!("/tmp/snapshot-{}", uuid::Uuid::new_v4());
        self.persistent.get_snapshot(path)
    }

    fn set_last_applied(&self, index: LogIndex) -> Result<(), <Command as CurpCommand>::Error> {
        _ = self
            .persistent
            .flush_ops(vec![WriteOp::PutAppliedIndex(index)])?;
        Ok(())
    }

    fn last_applied(&self) -> Result<LogIndex, <Command as CurpCommand>::Error> {
        let Some(index_bytes) = self.persistent.get_value(META_TABLE, APPLIED_INDEX_KEY)? else {
            return Ok(0);
        };
        let buf: [u8; 8] = index_bytes
            .try_into()
            .unwrap_or_else(|e| panic!("cannot decode index from backend, {e:?}"));
        Ok(u64::from_le_bytes(buf))
    }
}

/// Convert `CommandProposeError` to `tonic::Status`
pub(super) fn propose_err_to_status(err: CommandProposeError<Command>) -> tonic::Status {
    #[allow(clippy::wildcard_enum_match_arm)]
    match err {
        CommandProposeError::Execute(e) | CommandProposeError::AfterSync(e) => {
            // If an error occurs during the `prepare` or `execute` stages, `after_sync` will
            // not be invoked. In this case, `wait_synced` will return the errors generated
            // in the first two stages. Therefore, if the response from `slow_round` arrives
            // earlier than `fast_round`, the `propose` function will return a `SyncedError`,
            // even though `after_sync` is not called.
            tonic::Status::from(e)
        }
        CommandProposeError::Propose(ProposeError::SyncedError(e)) => {
            tonic::Status::unknown(e.to_string())
        }
        CommandProposeError::Propose(ProposeError::EncodeError(e)) => tonic::Status::internal(e),
        CommandProposeError::Propose(ProposeError::Timeout) => tonic::Status::internal("timeout"),

        _ => unreachable!("propose err {err:?}"),
    }
}
