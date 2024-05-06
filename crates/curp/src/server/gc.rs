use std::time::Duration;

use utils::task_manager::Listener;

use crate::{cmd::Command, rpc::ProposeId, server::cmd_board::CmdBoardRef};

use super::{conflict::spec_pool_new::SpeculativePoolRef, lease_manager::LeaseManagerRef};

pub(super) async fn gc_client_lease<C: Command>(
    lease_mamanger: LeaseManagerRef,
    cmd_board: CmdBoardRef<C>,
    sp: SpeculativePoolRef<C>,
    interval: Duration,
    shutdown_listener: Listener,
) {
    #[allow(clippy::arithmetic_side_effects, clippy::ignored_unit_patterns)]
    // introduced by tokio select
    loop {
        tokio::select! {
            _ = tokio::time::sleep(interval) => {}
            _ = shutdown_listener.wait() => break,
        }

        let mut lm_w = lease_mamanger.write();
        let mut board = cmd_board.write();
        let mut sp_l = sp.lock();
        let expired_ids = lm_w.gc_expired();

        let mut expired_propose_ids = Vec::new();
        for id in expired_ids {
            if let Some(tracker) = board.trackers.get(&id) {
                let incompleted_nums = tracker.all_incompleted();
                expired_propose_ids
                    .extend(incompleted_nums.into_iter().map(|num| ProposeId(id, num)))
            }
        }
        for id in &expired_propose_ids {
            let _ignore = board.er_buffer.swap_remove(id);
            let _ignore = board.asr_buffer.swap_remove(id);
            sp_l.remove_by_id(id);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use curp_test_utils::test_cmd::{TestCommand, TestCommandResult};
    use parking_lot::{Mutex, RwLock};
    use test_macros::abort_on_panic;
    use utils::task_manager::{tasks::TaskName, TaskManager};

    use crate::{
        rpc::{PoolEntry, ProposeId},
        server::{
            cmd_board::{CmdBoardRef, CommandBoard},
            conflict::spec_pool_new::SpeculativePool,
            gc::gc_client_lease,
            lease_manager::LeaseManager,
        },
    };

    #[tokio::test]
    #[abort_on_panic]
    async fn cmd_board_gc_test() {
        let task_manager = TaskManager::new();
        let board: CmdBoardRef<TestCommand> = Arc::new(RwLock::new(CommandBoard::new()));
        let lease_manager = Arc::new(RwLock::new(LeaseManager::new()));
        let sp = Arc::new(Mutex::new(SpeculativePool::new(vec![])));
        task_manager.spawn(TaskName::GcCmdBoard, |n| {
            gc_client_lease(
                lease_manager,
                Arc::clone(&board),
                sp,
                Duration::from_millis(500),
                n,
            )
        });

        tokio::time::sleep(Duration::from_millis(100)).await;
        board
            .write()
            .er_buffer
            .insert(ProposeId(1, 1), Ok(TestCommandResult::default()));
        tokio::time::sleep(Duration::from_millis(100)).await;
        board
            .write()
            .er_buffer
            .insert(ProposeId(2, 2), Ok(TestCommandResult::default()));
        board
            .write()
            .asr_buffer
            .insert(ProposeId(1, 1), Ok(0.into()));
        tokio::time::sleep(Duration::from_millis(100)).await;
        board
            .write()
            .asr_buffer
            .insert(ProposeId(2, 2), Ok(0.into()));

        // at 600ms
        tokio::time::sleep(Duration::from_millis(400)).await;
        board
            .write()
            .er_buffer
            .insert(ProposeId(3, 3), Ok(TestCommandResult::default()));
        board
            .write()
            .asr_buffer
            .insert(ProposeId(3, 3), Ok(0.into()));

        // at 1100ms, the first two kv should be removed
        tokio::time::sleep(Duration::from_millis(500)).await;
        let board = board.write();
        assert_eq!(board.er_buffer.len(), 1);
        assert_eq!(*board.er_buffer.get_index(0).unwrap().0, ProposeId(3, 3));
        assert_eq!(board.asr_buffer.len(), 1);
        assert_eq!(*board.asr_buffer.get_index(0).unwrap().0, ProposeId(3, 3));
        task_manager.shutdown(true).await;
    }

    #[tokio::test]
    #[abort_on_panic]
    async fn spec_gc_test() {
        let task_manager = TaskManager::new();
        let board: CmdBoardRef<TestCommand> = Arc::new(RwLock::new(CommandBoard::new()));
        let lease_manager = Arc::new(RwLock::new(LeaseManager::new()));
        let sp = Arc::new(Mutex::new(SpeculativePool::new(vec![])));
        task_manager.spawn(TaskName::GcCmdBoard, |n| {
            gc_client_lease(
                lease_manager,
                Arc::clone(&board),
                sp,
                Duration::from_millis(500),
                n,
            )
        });

        tokio::time::sleep(Duration::from_millis(100)).await;
        let cmd1 = Arc::new(TestCommand::default());
        sp.lock().insert(PoolEntry::new(ProposeId(0, 1), cmd1));

        tokio::time::sleep(Duration::from_millis(100)).await;
        let cmd2 = Arc::new(TestCommand::default());
        sp.lock().insert(PoolEntry::new(ProposeId(0, 2), cmd2));

        // at 600ms
        tokio::time::sleep(Duration::from_millis(400)).await;
        let cmd3 = Arc::new(TestCommand::default());
        sp.lock()
            .insert(PoolEntry::new(ProposeId(0, 3), Arc::clone(&cmd3)));

        // at 1100ms, the first two kv should be removed
        tokio::time::sleep(Duration::from_millis(500)).await;
        let spec = sp.lock();
        assert_eq!(spec.len(), 1);
        assert_eq!(spec.all(), vec![PoolEntry::new(ProposeId(0, 3), cmd3)]);
        task_manager.shutdown(true).await;
    }
}
