#![allow(
    clippy::mem_forget,
    clippy::pedantic,
    clippy::missing_inline_in_public_items,
    clippy::wildcard_imports,
    clippy::unwrap_used,
    clippy::multiple_inherent_impl
)]

use std::path::PathBuf;

use curp_test_utils::{test_cmd::TestCommand, TestRoleChange};
use utils::config::{CurpConfigBuilder, EngineConfig};

use super::*;
use crate::{
    member::MembershipInfo,
    rpc::{self, NodeMetadata},
    server::{
        cmd_board::CommandBoard,
        conflict::test_pools::{TestSpecPool, TestUncomPool},
    },
};

impl RawCurp<TestCommand, TestRoleChange> {
    pub fn new_bench(data_dir: PathBuf) -> Self {
        let n = 3;
        let cmd_board = Arc::new(RwLock::new(CommandBoard::new()));
        let curp_engine = EngineConfig::RocksDB(data_dir);
        let curp_config = CurpConfigBuilder::default()
            .log_entries_cap(10)
            .engine_cfg(curp_engine)
            .build()
            .unwrap();
        let curp_storage = Arc::new(DB::open(&curp_config.engine_cfg).unwrap());
        let _ignore = curp_storage.recover().unwrap();

        let sp = Arc::new(Mutex::new(SpeculativePool::new(
            vec![Box::<TestSpecPool>::default()],
            0,
        )));
        let ucp = Arc::new(Mutex::new(UncommittedPool::new(vec![
            Box::<TestUncomPool>::default(),
        ])));
        let (as_tx, as_rx) = flume::unbounded();
        std::mem::forget(as_rx);
        let resp_txs = Arc::new(Mutex::default());
        let id_barrier = Arc::new(IdBarrier::new());
        let init_members = (0..n)
            .map(|id| (id, NodeMetadata::new(format!("S{id}"), ["addr"], ["addr"])))
            .collect();
        let membership_info = MembershipInfo::new(0, init_members);
        let membership_config = MembershipConfig::Init(membership_info);
        let peer_addrs: HashMap<_, _> = membership_config
            .members()
            .clone()
            .into_iter()
            .map(|(id, meta)| (id, meta.into_peer_urls()))
            .collect();
        let member_connects = rpc::inner_connects(peer_addrs, None).collect();

        Self::builder()
            .is_leader(true)
            .cmd_board(cmd_board)
            .cfg(Arc::new(curp_config))
            .role_change(TestRoleChange::default())
            .task_manager(Arc::new(TaskManager::default()))
            .curp_storage(curp_storage)
            .spec_pool(sp)
            .uncommitted_pool(ucp)
            .as_tx(as_tx)
            .resp_txs(resp_txs)
            .id_barrier(id_barrier)
            .membership_config(membership_config)
            .member_connects(member_connects)
            .build_raw_curp()
            .unwrap()
    }

    pub fn bench_push_log_entries(&self, cmds: Vec<TestCommand>) {
        let entries = cmds
            .into_iter()
            .map(|c| (ProposeId::default(), Arc::new(c)));
        let _ignore = self.push_log_entries(entries);
    }

    pub fn bench_persistent_log_entries(&self, cmds: Vec<TestCommand>) {
        let entries: Vec<_> = cmds
            .into_iter()
            .map(|c| Arc::new(LogEntry::new(0, 0, ProposeId::default(), Arc::new(c))))
            .collect();
        Self::persistent_log_entries(self.storage(), entries);
    }

    pub fn bench_persistent_sp_entries(&self, cmds: Vec<TestCommand>) {
        let entries: Vec<_> = cmds
            .into_iter()
            .map(|c| PoolEntry::new(ProposeId::default(), Arc::new(c)))
            .collect();
        Self::persistent_sp_entries(self.storage(), entries);
    }

    pub fn bench_persistent_entries(&self, cmds: Vec<TestCommand>) {
        let entries: Vec<_> = cmds
            .into_iter()
            .map(|c| Arc::new(LogEntry::new(0, 0, ProposeId::default(), Arc::new(c))))
            .collect();
        Self::persistent_entries(self.storage(), entries);
    }
}
