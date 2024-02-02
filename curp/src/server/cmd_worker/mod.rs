//! `exe` stands for execution
//! `as` stands for after sync

use std::sync::Arc;

use tokio::sync::oneshot;
use tracing::{debug, error};

use super::raw_curp::RawCurp;
use crate::{
    cmd::{Command, CommandExecutor},
    log_entry::{EntryData, LogEntry},
    response::ResponseSender,
    role_change::RoleChange,
    rpc::{ConfChangeType, ProposeResponse, SyncedResponse},
    snapshot::{Snapshot, SnapshotMeta},
};

/// Cmd worker execute handler
pub(super) async fn execute<C: Command, CE: CommandExecutor<C>, RC: RoleChange>(
    entry: Arc<LogEntry<C>>,
    ce: &CE,
    curp: &RawCurp<C, RC>,
) -> Result<<C as Command>::ER, <C as Command>::Error> {
    let (sp, ucp) = (curp.spec_pool(), curp.uncommitted_pool());
    let id = curp.id();
    match entry.entry_data {
        EntryData::Command(ref cmd) => {
            let er = ce.mock_exe(cmd).await;
            if er.is_err() {
                sp.lock().remove(&entry.propose_id);
                let _ig = ucp.lock().remove(&entry.propose_id);
                ce.trigger(entry.inflight_id(), entry.index);
            }
            debug!(
                "{id} cmd({}) is speculatively executed, exe status: {}",
                entry.propose_id,
                er.is_ok(),
            );
            er
        }
        EntryData::ConfChange(_)
        | EntryData::Shutdown
        | EntryData::Empty
        | EntryData::SetName(_, _) => {
            unreachable!("should not speculative execute {:?}", entry.entry_data)
        }
    }
}

/// Cmd worker after sync handler
pub(super) async fn after_sync<C: Command, CE: CommandExecutor<C>, RC: RoleChange>(
    entry: Arc<LogEntry<C>>,
    resp_tx: Option<Arc<ResponseSender>>,
    ce: &CE,
    curp: &RawCurp<C, RC>,
) {
    let (cb, sp, ucp) = (curp.cmd_board(), curp.spec_pool(), curp.uncommitted_pool());
    let id = curp.id();
    let remove_from_sp_ucp = || {
        sp.lock().remove(&entry.propose_id);
        let _ig = ucp.lock().remove(&entry.propose_id);
    };
    #[allow(clippy::pattern_type_mismatch)]
    match (&entry.entry_data, resp_tx) {
        // Leader
        (EntryData::Command(ref cmd), Some(ref tx)) => {
            if tx.is_conflict() {
                let er = ce.mock_exe(cmd.as_ref()).await;
                tx.send_propose(ProposeResponse::new_result::<C>(&er, true));
                if er.is_err() {
                    ce.trigger(entry.inflight_id(), entry.index);
                    remove_from_sp_ucp();
                    return;
                }
            }
            let asr = ce.after_sync(cmd.as_ref(), entry.index).await;
            tx.send_synced(SyncedResponse::new_result::<C>(&asr));
            remove_from_sp_ucp();
        }
        // Follower
        (EntryData::Command(ref cmd), None) => {
            let _ignore = ce.after_sync(cmd.as_ref(), entry.index).await;
            remove_from_sp_ucp();
        }
        (EntryData::Shutdown, _) => {
            curp.enter_shutdown();
            if curp.is_leader() {
                curp.shutdown_trigger().mark_leader_notified();
            }
            if let Err(e) = ce.set_last_applied(entry.index) {
                error!("failed to set last_applied, {e}");
            }
            cb.write().notify_shutdown();
        }
        (EntryData::ConfChange(ref conf_change), _) => {
            if let Err(e) = ce.set_last_applied(entry.index) {
                error!("failed to set last_applied, {e}");
                return;
            }
            let change = conf_change.first().unwrap_or_else(|| {
                unreachable!("conf change should always have at least one change")
            });
            let shutdown_self =
                change.change_type() == ConfChangeType::Remove && change.node_id == id;
            cb.write().insert_conf(entry.propose_id);
            remove_from_sp_ucp();
            if shutdown_self {
                curp.shutdown_trigger().self_shutdown();
            }
        }
        (EntryData::SetName(node_id, ref name), _) => {
            if let Err(e) = ce.set_last_applied(entry.index) {
                error!("failed to set last_applied, {e}");
                return;
            }
            curp.cluster().set_name(*node_id, name.clone());
        }
        (EntryData::Empty, _) => {}
    };
    debug!("{id} cmd({}) after sync is called", entry.propose_id);
    ce.trigger(entry.inflight_id(), entry.index);
}

/// Cmd worker reset handler
pub(super) async fn worker_reset<C: Command, CE: CommandExecutor<C>, RC: RoleChange>(
    snapshot: Option<Snapshot>,
    finish_tx: oneshot::Sender<()>,
    ce: &CE,
    curp: &RawCurp<C, RC>,
) -> bool {
    let id = curp.id();
    if let Some(snapshot) = snapshot {
        let meta = snapshot.meta;
        #[allow(clippy::expect_used)] // only in debug
        if let Err(e) = ce
            .reset(Some((snapshot.into_inner(), meta.last_included_index)))
            .await
        {
            error!("reset failed, {e}");
        } else {
            debug_assert_eq!(
                ce.last_applied()
                    .expect("failed to get last_applied from ce"),
                meta.last_included_index,
                "inconsistent last_applied"
            );
            debug!("{id}'s command executor has been reset by a snapshot");
            curp.reset_by_snapshot(meta);
        }
    } else {
        if let Err(e) = ce.reset(None).await {
            error!("reset failed, {e}");
        }
        debug!("{id}'s command executor has been restored to the initial state");
    }
    let _ig = finish_tx.send(());
    true
}

/// Cmd worker snapshot handler
pub(super) async fn worker_snapshot<C: Command, CE: CommandExecutor<C>, RC: RoleChange>(
    meta: SnapshotMeta,
    tx: oneshot::Sender<Snapshot>,
    ce: &CE,
    curp: &RawCurp<C, RC>,
) -> bool {
    match ce.snapshot().await {
        Ok(snapshot) => {
            debug_assert!(
                ce.last_applied()
                    .is_ok_and(|last_applied| last_applied <= meta.last_included_index),
                " the `last_as` should always be less than or equal to the `last_exe`"
            ); // sanity check
            let snapshot = Snapshot::new(meta, snapshot);
            debug!("{} takes a snapshot, {snapshot:?}", curp.id());
            if tx.send(snapshot).is_err() {
                error!("snapshot oneshot closed");
            }
            true
        }
        Err(e) => {
            error!("snapshot failed, {e}");
            false
        }
    }
}
