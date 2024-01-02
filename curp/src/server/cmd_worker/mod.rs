//! `exe` stands for execution
//! `as` stands for after sync

use std::sync::Arc;

use tokio::sync::oneshot;
use tracing::{debug, error};

use super::raw_curp::RawCurp;
use crate::{
    cmd::{Command, CommandExecutor},
    log_entry::{EntryData, LogEntry},
    role_change::RoleChange,
    rpc::ConfChangeType,
    server::cmd_board::CommandBoard,
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
            let er = ce.execute(cmd).await;
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
    should_execute: bool,
    ce: &CE,
    curp: &RawCurp<C, RC>,
) {
    let (cb, sp, ucp) = (curp.cmd_board(), curp.spec_pool(), curp.uncommitted_pool());
    let id = curp.id();
    match entry.entry_data {
        EntryData::Command(ref cmd) => {
            if should_execute {
                let er_err = match CommandBoard::wait_for_er(&cb, entry.propose_id).await {
                    Some(er) => er.is_err(),
                    None => {
                        let er = ce.execute(cmd.as_ref()).await;
                        let er_ok = er.is_err();
                        cb.write().insert_er(entry.propose_id, er);
                        er_ok
                    }
                };
                if er_err {
                    ce.trigger(entry.inflight_id(), entry.index);
                    return;
                }
            }
            let asr = ce.after_sync(cmd.as_ref(), entry.index).await;
            cb.write().insert_asr(entry.propose_id, asr);
            sp.lock().remove(&entry.propose_id);
            let _ig = ucp.lock().remove(&entry.propose_id);
            debug!("{id} cmd({}) after sync is called", entry.propose_id);
        }
        EntryData::Shutdown => {
            curp.enter_shutdown();
            if curp.is_leader() {
                curp.shutdown_trigger().mark_leader_notified();
            }
            if let Err(e) = ce.set_last_applied(entry.index) {
                error!("failed to set last_applied, {e}");
            }
            cb.write().notify_shutdown();
        }
        EntryData::ConfChange(ref conf_change) => {
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
            sp.lock().remove(&entry.propose_id);
            let _ig = ucp.lock().remove(&entry.propose_id);
            if shutdown_self {
                curp.shutdown_trigger().self_shutdown();
            }
        }
        EntryData::SetName(node_id, ref name) => {
            if let Err(e) = ce.set_last_applied(entry.index) {
                error!("failed to set last_applied, {e}");
                return;
            }
            curp.cluster().set_name(node_id, name.clone());
        }
        EntryData::Empty => {}
    };
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
