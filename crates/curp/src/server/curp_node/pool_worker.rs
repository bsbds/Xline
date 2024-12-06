use std::sync::Arc;

use clippy_utilities::NumericCast;
use curp_external_api::{
    cmd::{Command, CommandExecutor},
    role_change::RoleChange,
};
use opentelemetry::KeyValue;
use tracing::warn;

use crate::{
    rpc::{PoolEntry, ProposeId},
    server::{
        conflict::{
            spec_pool_new::{SpecPoolRepl, SpeculativePool},
            uncommitted_pool::UncommittedPool,
        },
        metrics, RawCurp,
    },
};

use super::CurpNode;

/// A oneshot sender
type Sender<T> = crossbeam_channel::Sender<T>;

/// Operations that can be performed on the conflict pools
pub(crate) enum PoolOp<C> {
    /// Insert entries into the leader's pool
    LeaderRecord(Vec<PoolEntry<C>>, Sender<ConflictCheckResult>),
    /// Insert entries into the follower's pool
    FollowerRecord(Vec<PoolEntry<C>>, Sender<ConflictCheckResult>),
    /// Remove entries
    Remove(Vec<PoolEntry<C>>),
    /// Gets all ids in speculative pool
    GetSpIds(Sender<SpecPoolRepl>),
    /// Garbage collect speculative pool
    GcSp(SpecPoolRepl, Sender<Vec<ProposeId>>),
}

/// Result of conflict checking for a batch of commands
#[derive(Debug)]
pub(crate) struct ConflictCheckResult {
    /// Vector of conflict flags - true indicates a conflict was found
    pub(crate) conflicts: Vec<bool>,
    /// Version number used for conflict checking
    pub(crate) version: u64,
}

impl ConflictCheckResult {
    /// Creates a new `ConflictCheckResult`
    fn new(conflicts: Vec<bool>, version: u64) -> Self {
        Self { conflicts, version }
    }
}

impl<C: Command, CE: CommandExecutor<C>, RC: RoleChange> CurpNode<C, CE, RC> {
    /// Worker for conflict pool operations
    #[allow(clippy::needless_pass_by_value)]
    pub(crate) fn pool_worker(rx: flume::Receiver<PoolOp<C>>, curp: Arc<RawCurp<C, RC>>) {
        let (mut sp, mut ucp) = (curp.spec_pool().lock(), curp.uncommitted_pool().lock());
        while let Ok(op) = rx.recv() {
            match op {
                PoolOp::LeaderRecord(entries, tx) => {
                    let resp = Self::leader_record(&mut sp, &mut ucp, entries);
                    let _ignore = tx.send(resp);
                }
                PoolOp::Remove(entries) => Self::remove(&mut sp, &mut ucp, entries),
                PoolOp::FollowerRecord(entries, tx) => {
                    let resp = Self::follower_record(&mut sp, entries);
                    let _ignore = tx.send(resp);
                }
                PoolOp::GetSpIds(tx) => {
                    let ids = sp.all_ids().copied().collect();
                    let next_version = sp.version().wrapping_add(1);
                    let entry = SpecPoolRepl::new(next_version, ids);
                    let _ignore = tx.send(entry);
                }
                PoolOp::GcSp(l, tx) => {
                    let removed = sp.gc(l.ids(), l.version());
                    let _ignore = tx.send(removed);
                }
            }
        }
        warn!("pool worker exits");
    }

    /// Handles remove
    fn remove(
        sp_l: &mut SpeculativePool<C>,
        ucp_l: &mut UncommittedPool<C>,
        entries: Vec<PoolEntry<C>>,
    ) {
        let entries_c = entries.clone();
        rayon::join(
            || {
                for entry in entries {
                    sp_l.remove(&entry);
                }
            },
            || {
                for entry in entries_c {
                    ucp_l.remove(&entry);
                }
            },
        );
    }

    /// Handles leader record
    fn leader_record(
        sp_l: &mut SpeculativePool<C>,
        ucp_l: &mut UncommittedPool<C>,
        entries: Vec<PoolEntry<C>>,
    ) -> ConflictCheckResult {
        let entries_c = entries.clone();
        let ((a, version), b) = rayon::join(
            || {
                let cs = entries
                    .into_iter()
                    .map(|e| sp_l.insert(e).is_some())
                    .collect::<Vec<_>>();
                (cs, sp_l.version())
            },
            || {
                entries_c
                    .into_iter()
                    .map(|e| ucp_l.insert(&e))
                    .collect::<Vec<_>>()
            },
        );
        let conflicts: Vec<_> = a.into_iter().zip(b).map(|(aa, bb)| aa | bb).collect();
        metrics::get().proposals_failed.add(
            conflicts.iter().filter(|c| **c).count().numeric_cast(),
            &[KeyValue::new("reason", "leader key conflict")],
        );

        ConflictCheckResult::new(conflicts, version)
    }

    /// Handles follower record
    fn follower_record(
        sp: &mut SpeculativePool<C>,
        entries: Vec<PoolEntry<C>>,
    ) -> ConflictCheckResult {
        let conflicts = entries
            .into_iter()
            .map(|e| sp.insert(e).is_some())
            .collect::<Vec<_>>();
        metrics::get().proposals_failed.add(
            conflicts.iter().filter(|c| **c).count().numeric_cast(),
            &[KeyValue::new("reason", "follower key conflict")],
        );

        ConflictCheckResult::new(conflicts, sp.version())
    }
}
