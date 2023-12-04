//* Power failure fault means that only the last unsynced data might be lost.
//* This is to test if all the checkpoints are valid.

use super::*;

#[test]
fn wal_wont_lost_persistent_index_on_power_failure() {
    init_logger();
    let dir = init_wal_dir();
    let config = TestConfig::new_power_failure();
    let entry_gen = Arc::new(EntryGenerator::new(TEST_SEGMENT_SIZE));

    let mut min_persistent_index = 0;

    for test_count in 0..=config.wal_recovery_count {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap();
        let dir_c = dir.clone();
        let entry_gen_c = Arc::clone(&entry_gen);

        let res = rt.block_on(async move {
            let (wal_storage, logs) = match recover_wal_storage(&dir_c).await {
                Ok(res) => res,
                Err(e) => {
                    warn!("failed to recover wal storage: {e}, aborting test");
                    return None;
                }
            };
            let last_recovered_index = logs.last().map(|e| e.index).unwrap_or(0);
            info!("min persistent index: {min_persistent_index}");
            info!("last recoverd index: {}", last_recovered_index);
            assert!(
                last_recovered_index >= min_persistent_index,
                "recovered log index should be larger or equal to min persistent index"
            );
            logs_expect(&entry_gen_c.all_logs(), &logs);

            if test_count == config.wal_recovery_count {
                return None;
            }

            entry_gen_c.reset_next_index_to(last_recovered_index + 1);
            let shutdown_flag = Arc::new(AtomicBool::new(false));

            let send_handle = tokio::spawn(log_persistent_task(
                wal_storage,
                Arc::clone(&entry_gen_c),
                Arc::clone(&shutdown_flag),
                config.log_send_interval,
                config.head_truncation_interval,
                config.tail_truncation_interval,
            ));

            tokio::time::sleep(config.log_persistent_duration).await;
            shutdown_flag.store(true, Ordering::SeqCst);
            let min_persistent_index = send_handle.await.unwrap();
            Some(min_persistent_index)
        });
        if let Some(index) = res {
            min_persistent_index = index;
        } else {
            break;
        }
    }
}
