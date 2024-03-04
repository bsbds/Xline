//! `exe` stands for execution
//! `as` stands for after sync

use std::sync::Arc;

use parking_lot::Mutex;
use tokio::sync::oneshot;
use tracing::{debug, error, info, warn};

use super::{
    conflict::{spec_pool_new::SpecPool, uncommitted_pool::UncomPool},
    raw_curp::RawCurp,
};
use crate::{
    cmd::{Command, CommandExecutor},
    log_entry::{EntryData, LogEntry},
    response::ResponseSender,
    role_change::RoleChange,
    rpc::{ConfChangeType, PoolEntry, ProposeResponse, SyncedResponse},
    snapshot::{Snapshot, SnapshotMeta},
};

/// Removes an entry from sp and ucp
fn remove_from_sp_ucp<C: Command>(
    sp: &Mutex<SpecPool<C>>,
    ucp: &Mutex<UncomPool<C>>,
    entry: &LogEntry<C>,
) {
    let pool_entry = match entry.entry_data {
        EntryData::Command(ref c) => PoolEntry::new(entry.propose_id, Arc::clone(c)),
        EntryData::ConfChange(ref c) => PoolEntry::new(entry.propose_id, c.clone()),
        EntryData::Empty | EntryData::Shutdown | EntryData::SetNodeState(_, _, _) => {
            unreachable!()
        }
    };
    sp.lock().remove(pool_entry.clone());
    ucp.lock().remove(pool_entry);
}

/// Cmd worker execute handler
pub(super) async fn execute<C: Command, CE: CommandExecutor<C>, RC: RoleChange>(
    entry: Arc<LogEntry<C>>,
    ce: &CE,
    curp: &RawCurp<C, RC>,
) -> Result<<C as Command>::ER, <C as Command>::Error> {
    let (sp, ucp) = (curp.new_spec_pool(), curp.new_uncommitted_pool());
    let id = curp.id();
    match entry.entry_data {
        EntryData::Command(ref cmd) => {
            let er = ce.execute(cmd).await;
            if er.is_err() {
                remove_from_sp_ucp(&sp, &ucp, &entry);
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
        | EntryData::SetNodeState(_, _, _) => {
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
    let (cb, sp, ucp) = (
        curp.cmd_board(),
        curp.new_spec_pool(),
        curp.new_uncommitted_pool(),
    );
    let id = curp.id();
    #[allow(clippy::pattern_type_mismatch)]
    match (&entry.entry_data, resp_tx) {
        // Leader
        (EntryData::Command(ref cmd), Some(ref tx)) => {
            if tx.is_conflict() {
                let er = ce.execute(cmd.as_ref()).await;
                tx.send_propose(ProposeResponse::new_result::<C>(&er, true));
                if er.is_err() {
                    ce.trigger(entry.inflight_id(), entry.index);
                    remove_from_sp_ucp(&sp, &ucp, &entry);
                    return;
                }
            }
            let asr = ce.after_sync(cmd.as_ref(), entry.index).await;
            tx.send_synced(SyncedResponse::new_result::<C>(&asr));
            remove_from_sp_ucp(&sp, &ucp, &entry);
        }
        // Follower
        (EntryData::Command(ref cmd), None) => {
            let _ignore = ce.after_sync(cmd.as_ref(), entry.index).await;
            remove_from_sp_ucp(&sp, &ucp, &entry);
        }
        (EntryData::Shutdown, _) => {
            curp.task_manager().cluster_shutdown();
            if curp.is_leader() {
                curp.task_manager().mark_leader_notified();
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
            remove_from_sp_ucp(&sp, &ucp, &entry);
            if shutdown_self {
                if let Some(maybe_new_leader) = curp.pick_new_leader() {
                    info!(
                        "the old leader {} will shutdown, try to move leadership to {}",
                        id, maybe_new_leader
                    );
                    if curp
                        .handle_move_leader(maybe_new_leader)
                        .unwrap_or_default()
                    {
                        if let Err(e) = curp
                            .connects()
                            .get(&maybe_new_leader)
                            .unwrap_or_else(|| {
                                unreachable!("connect to {} should exist", maybe_new_leader)
                            })
                            .try_become_leader_now(curp.cfg().wait_synced_timeout)
                            .await
                        {
                            warn!(
                                "{} send try become leader now to {} failed: {:?}",
                                curp.id(),
                                maybe_new_leader,
                                e
                            );
                        };
                    }
                } else {
                    info!(
                        "the old leader {} will shutdown, but no other node can be the leader now",
                        id
                    );
                }
                curp.task_manager().shutdown(false).await;
            }
        }
        (EntryData::SetNodeState(node_id, ref name, ref client_urls), _) => {
            if let Err(e) = ce.set_last_applied(entry.index) {
                error!("failed to set last_applied, {e}");
                return;
            }
            curp.cluster()
                .set_node_state(*node_id, name.clone(), client_urls.clone());
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
