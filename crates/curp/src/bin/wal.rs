use std::sync::Arc;

use curp::{
    log_entry::LogEntry,
    rpc::ProposeId,
    server::storage::wal::{
        codec::{DataFrame, DataFrameOwned},
        config::WALConfig,
        WALStorage,
    },
    EntryData,
};
use curp_test_utils::test_cmd::TestCommand;

#[tokio::main]
async fn main() {
    std::fs::remove_dir_all("/home/gum/wal-bench").unwrap();
    let config = WALConfig::new("/home/gum/wal-bench").with_max_segment_size(0x4000000);
    let mut storage = WALStorage::<TestCommand>::new(config.clone()).unwrap();
    storage.recover().await.unwrap();
    for i in 0..100 {
        let entries: Vec<_> = (0..500)
            .map(|i| {
                let cmd = TestCommand::new_put(vec![0; 256], i);
                DataFrameOwned::Entry(LogEntry::new(
                    0,
                    0,
                    ProposeId(0, 0),
                    EntryData::Command(Arc::new(cmd)),
                ))
            })
            .collect();
        let frames = entries.iter().map(|e| e.get_ref()).collect();
        let start = std::time::Instant::now();
        storage.send_sync(frames).await.unwrap();
        println!("takes: {:?}", start.elapsed());
    }
}
