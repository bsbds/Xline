use curp_external_api::cmd::ProposeId;
use curp_test_utils::test_cmd::TestCommand;
use tempfile::TempDir;

use crate::{
    log_entry::{EntryData, LogEntry},
    server::storage::wal::codec::DataFrame,
};

use super::*;

#[tokio::test(flavor = "multi_thread")]
async fn log_append_and_recovery_is_ok() -> io::Result<()> {
    let wal_test_path = tempfile::tempdir().unwrap();
    let (mut storage, _logs) = FramedWALStorage::new_or_recover(&wal_test_path)
        .await
        .unwrap();

    let num_entries = 10;

    let entries = (1..=10)
        .map(|index| LogEntry::<TestCommand>::new(index, 1, EntryData::Empty(ProposeId(1, 2))));

    for entry in entries.clone() {
        storage
            .send_sync(vec![DataFrame::Entry(entry.clone())])
            .await
            .unwrap();
    }

    drop(storage);

    let (_storage, logs) = FramedWALStorage::<TestCommand>::new_or_recover(&wal_test_path)
        .await
        .unwrap();

    assert_eq!(logs.len(), num_entries, "failed to recover all logs");
    assert!(
        logs.into_iter().zip(entries).all(|(x, y)| x == y),
        "log entries mismatched"
    );

    Ok(())
}
