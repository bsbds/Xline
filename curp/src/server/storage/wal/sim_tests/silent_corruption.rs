//* Silent corruption faults means that the any part of the logs can be corrupted
//* We test that if all logs recovered are consecutive and contains valid data
use super::*;

#[test]
fn log_recovery_on_lazyfs_silent_corruption() {
    init_logger();
    let dir = init_wal_dir();
    let config = TestConfig::new_silent_corruption();
    let entry_gen = Arc::new(EntryGenerator::new(TEST_SEGMENT_SIZE));

    for test_count in 0..=config.wal_recovery_count {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap();
        let dir_c = dir.clone();
        let entry_gen_c = Arc::clone(&entry_gen);

        rt.block_on(async move {
            let (wal_storage, logs_recovered) = match recover_wal_storage(&dir_c).await {
                Ok(res) => res,
                Err(e) => {
                    warn!("failed to recover wal storage: {e}, aborting test");
                    return;
                }
            };
            let logs_sent = entry_gen_c.all_logs();
            info!("logs sent: {}", logs_sent.len());
            info!(
                "logs recovered: {}, start: {:?}, end: {:?}",
                logs_recovered.len(),
                logs_recovered.first(),
                logs_recovered.last(),
            );
            logs_expect(&logs_sent, &logs_recovered);

            if test_count == config.wal_recovery_count {
                return;
            }

            let last_recovered_index = logs_recovered.last().map_or(0, |e| e.index);
            entry_gen_c.reset_next_index_to(last_recovered_index + 1);

            let shutdown_flag = Arc::new(AtomicBool::new(false));
            let send_handle = tokio::spawn(log_persistent_task(
                wal_storage,
                entry_gen_c,
                Arc::clone(&shutdown_flag),
                config.log_send_interval,
                config.head_truncation_interval,
                config.tail_truncation_interval,
            ));
            let failure_handle = tokio::spawn(faults_injection_task(
                dir_c,
                config.fault_injection_interval,
                config.bitflip_spray_percentage,
                Arc::clone(&shutdown_flag),
                vec![Faults::DropUnsync, Faults::BitFlip],
            ));

            tokio::time::sleep(config.log_persistent_duration).await;
            shutdown_flag.store(true, Ordering::Relaxed);
            let _index = send_handle.await.unwrap();
        });
    }
}
