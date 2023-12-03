use bytes::BytesMut;
use curp_external_api::cmd::ProposeId;
use curp_test_utils::test_cmd::TestCommand;
use tempfile::TempDir;
use tokio_util::codec::Encoder;

use crate::{
    log_entry::{EntryData, LogEntry},
    server::storage::wal::{codec::DataFrame, util::get_file_paths_with_ext},
};

use super::*;

#[tokio::test(flavor = "multi_thread")]
async fn log_append_and_recovery_is_ok() {
    let wal_test_path = tempfile::tempdir().unwrap();
    let config = WALConfig::new(wal_test_path);
    let (mut storage, _logs) = WALStorage::new_or_recover(config.clone()).await.unwrap();

    let num_entries = 10;

    let entries = (1..=num_entries)
        .map(|index| LogEntry::<TestCommand>::new(index, 1, EntryData::Empty(ProposeId(1, 2))));

    for entry in entries.clone() {
        storage
            .send_sync(vec![DataFrame::Entry(entry.clone())])
            .await
            .unwrap();
    }

    drop(storage);

    let (_storage, logs) = WALStorage::<TestCommand>::new_or_recover(config)
        .await
        .unwrap();

    assert_eq!(
        logs.len(),
        num_entries as usize,
        "failed to recover all logs"
    );
    assert!(
        logs.into_iter().zip(entries).all(|(x, y)| x == y),
        "log entries mismatched"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn log_tail_truncation_is_ok() {
    let wal_test_path = tempfile::tempdir().unwrap();
    let config = WALConfig::new(wal_test_path);
    let (mut storage, _logs) = WALStorage::new_or_recover(config.clone()).await.unwrap();

    let entries = (1..=10)
        .map(|index| LogEntry::<TestCommand>::new(index, 1, EntryData::Empty(ProposeId(1, 2))));

    for entry in entries.clone() {
        storage
            .send_sync(vec![DataFrame::Entry(entry.clone())])
            .await
            .unwrap();
    }

    storage.truncate_tail(7).await;
    let next_entry = LogEntry::<TestCommand>::new(8, 1, EntryData::Empty(ProposeId(1, 3)));
    storage
        .send_sync(vec![DataFrame::Entry(next_entry.clone())])
        .await
        .unwrap();

    drop(storage);

    let (_storage, logs) = WALStorage::<TestCommand>::new_or_recover(config)
        .await
        .unwrap();

    assert_eq!(logs.len(), 8, "failed to recover all logs");

    assert_eq!(*logs.last().unwrap(), next_entry);
}

#[tokio::test(flavor = "multi_thread")]
async fn log_head_truncation_is_ok() {
    let wal_test_path = tempfile::tempdir().unwrap();
    let max_segment_size = 512;
    let header_size = 32;

    let num_wals = || {
        get_file_paths_with_ext(&wal_test_path, ".wal")
            .unwrap()
            .len()
    };

    let sample_entry = LogEntry::<TestCommand>::new(1, 1, EntryData::Empty(ProposeId(1, 2)));
    let mut wal_codec = Wal::<TestCommand>::new();
    let mut buf = BytesMut::new();
    wal_codec.encode(vec![DataFrame::Entry(sample_entry)], &mut buf);
    let entry_size = buf.len();

    let num_entries_per_segment = (max_segment_size - header_size + entry_size - 1) / entry_size;

    let config = WALConfig::new(&wal_test_path).with_max_segment_size(max_segment_size as u64);
    let (mut storage, _logs) = WALStorage::new_or_recover(config.clone()).await.unwrap();

    let mut frame_gen = FrameGenerator::new();
    for frame in frame_gen.take(num_entries_per_segment + 1) {
        storage.send_sync(vec![frame]).await.unwrap();
    }

    assert_eq!(num_wals(), 2);
    storage
        .truncate_head(num_entries_per_segment as u64 + 1)
        .await;
    assert_eq!(num_wals(), 1);
}

#[derive(Clone)]
struct FrameGenerator {
    next_index: u64,
}

impl FrameGenerator {
    fn new() -> Self {
        Self { next_index: 1 }
    }

    fn next(&mut self) -> DataFrame<TestCommand> {
        let entry =
            LogEntry::<TestCommand>::new(self.next_index, 1, EntryData::Empty(ProposeId(1, 2)));
        self.next_index += 1;

        DataFrame::Entry(entry)
    }

    fn take(&mut self, num: usize) -> Vec<DataFrame<TestCommand>> {
        (0..num).map(|_| self.next()).collect()
    }
}
