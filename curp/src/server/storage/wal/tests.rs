use std::{path::Path, sync::Arc};

use bytes::BytesMut;
use curp_test_utils::test_cmd::TestCommand;
use parking_lot::Mutex;
use tempfile::TempDir;
use tokio_util::codec::Encoder;

use crate::{
    log_entry::{EntryData, LogEntry},
    rpc::ProposeId,
    server::storage::wal::{
        codec::DataFrameOwned, test_util::EntryGenerator, util::get_file_paths_with_ext,
    },
};

use super::*;

const TEST_SEGMENT_SIZE: u64 = 512;

#[tokio::test(flavor = "multi_thread")]
async fn simple_append_and_recovery_is_ok() {
    let wal_test_path = tempfile::tempdir().unwrap();
    test_follow_up_append_recovery(wal_test_path.path(), 100);
}

#[tokio::test(flavor = "multi_thread")]
async fn log_head_truncation_is_ok() {
    let wal_test_path = tempfile::tempdir().unwrap();
    for num_entries in 1..100 {
        for truncate_at in 1..=num_entries {
            test_head_truncate_at(wal_test_path.path(), num_entries, truncate_at as u64);
            test_follow_up_append_recovery(wal_test_path.path(), 10);
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn log_tail_truncation_is_ok() {
    let wal_test_path = tempfile::tempdir().unwrap();
    for num_entries in 1..100 {
        for truncate_at in 1..=num_entries {
            test_tail_truncate_at(wal_test_path.path(), num_entries, truncate_at as u64);
            test_follow_up_append_recovery(wal_test_path.path(), 10);
        }
    }
}

/// Checks if the segment files are deleted
async fn test_head_truncate_at(wal_test_path: &Path, num_entries: usize, truncate_at: LogIndex) {
    let get_num_segments = || {
        get_file_paths_with_ext(&wal_test_path, ".wal")
            .unwrap()
            .len()
    };

    let config = WALConfig::new(&wal_test_path, TEST_SEGMENT_SIZE);
    let (mut storage, _logs) = WALStorage::<TestCommand>::new_or_recover(config.clone())
        .await
        .unwrap();

    let mut entry_gen = EntryGenerator::new(TEST_SEGMENT_SIZE);
    let num_entries_per_segment = entry_gen.num_entries_per_segment();

    for entry in entry_gen.take(num_entries) {
        storage.send_sync(vec![&entry]).await.unwrap();
    }

    let num_segments = (num_entries + num_entries_per_segment - 1) / num_entries_per_segment;
    assert_eq!(num_segments, get_num_segments());

    storage.truncate_head(truncate_at).await.unwrap();

    let num_entries_truncated = num_entries - truncate_at as usize;
    let num_segments_truncated =
        (num_entries_truncated + num_entries_per_segment - 1) / num_entries_per_segment;
    assert_eq!(num_segments_truncated, get_num_segments());
}

async fn test_tail_truncate_at(wal_test_path: &Path, num_entries: usize, truncate_at: LogIndex) {
    assert!(num_entries as u64 >= truncate_at);
    let config = WALConfig::new(&wal_test_path, TEST_SEGMENT_SIZE);
    let (mut storage, _logs) = WALStorage::new_or_recover(config.clone()).await.unwrap();

    let mut entry_gen = EntryGenerator::new(TEST_SEGMENT_SIZE);
    for entry in entry_gen.take(num_entries) {
        storage.send_sync(vec![&entry]).await.unwrap();
    }

    storage.truncate_tail(truncate_at).await;
    let next_entry =
        LogEntry::<TestCommand>::new(truncate_at + 1, 1, ProposeId(1, 3), EntryData::Empty);
    storage.send_sync(vec![&next_entry]).await.unwrap();

    drop(storage);

    let (_storage, logs) = WALStorage::<TestCommand>::new_or_recover(config)
        .await
        .unwrap();

    assert_eq!(
        logs.len() as u64,
        truncate_at + 1,
        "failed to recover all logs"
    );

    assert_eq!(*logs.last().unwrap(), next_entry);
}

/// Test if the append and recovery are ok after some event
async fn test_follow_up_append_recovery(wal_test_path: &Path, to_append: usize) {
    let config = WALConfig::new(&wal_test_path, TEST_SEGMENT_SIZE);
    let (mut storage, logs_initial) = WALStorage::<TestCommand>::new_or_recover(config.clone())
        .await
        .unwrap();

    let next_log_index = logs_initial.last().map_or(0, |e| e.index) + 1;

    let mut entry_gen = EntryGenerator::new(TEST_SEGMENT_SIZE);
    entry_gen.skip(logs_initial.len());
    let entries = entry_gen.take(to_append);

    for entry in entries.clone() {
        storage.send_sync(vec![&entry]).await.unwrap();
    }

    drop(storage);

    let (_storage, logs) = WALStorage::<TestCommand>::new_or_recover(config)
        .await
        .unwrap();

    assert_eq!(
        logs.len(),
        logs_initial.len() + to_append,
        "failed to recover all logs"
    );

    assert!(
        logs.into_iter()
            .skip(logs_initial.len())
            .zip(entries)
            .all(|(x, y)| x == y),
        "log entries mismatched"
    );
}
