mod codec;

/// File pipeline
mod file_pipeline;

mod util;

use std::{fs::OpenOptions, io, path::PathBuf};

use fs2::FileExt;
use serde::Serialize;
use tokio::fs::File as TokioFile;
use tokio_util::codec::Framed;

use self::codec::WAL;

const SEGMENT_SIZE_BYTES: u64 = 64 * 1024 * 1024;

/// The log storage
#[derive(Debug)]
pub(super) struct LogStorage {
    /// The directory to store the log files
    dir: PathBuf,
    /// Locked files
    lfiles: Vec<TokioFile>,
}

impl LogStorage {
    /// Creates a new `LogStorage`
    pub(super) async fn new(dir: &str) -> io::Result<Self> {
        let dir = PathBuf::from(dir);
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(dir.join(Self::log_name(0, 0)))?;
        file.try_lock_exclusive()?;

        Ok(Self {
            dir,
            lfiles: vec![TokioFile::from_std(file)],
        })
    }

    pub(crate) fn framed_io<C: Serialize>(&mut self) -> Framed<TokioFile, WAL<C>> {
        let file = self.lfiles.pop().unwrap();
        Framed::new(file, WAL::<C>::new())
    }

    /// Gets the name of the log file
    fn log_name(seq: u64, log_index: u64) -> String {
        format!("{seq}-{log_index}.log")
    }
}

#[cfg(test)]
mod tests {
    use curp_external_api::cmd::ProposeId;
    use curp_test_utils::test_cmd::TestCommand;
    use futures::SinkExt;
    use tokio_stream::StreamExt;

    use crate::{
        log_entry::{EntryData, LogEntry},
        server::storage::wal::codec::DataFrame,
    };

    use super::*;

    #[tokio::test(flavor = "multi_thread")]
    async fn append_and_read_is_ok() -> io::Result<()> {
        let mut storage = LogStorage::new("/tmp/wal").await.unwrap();
        let mut io = storage.framed_io();
        let frame = DataFrame::Entry(LogEntry::<TestCommand>::new(
            1,
            1,
            EntryData::Empty(ProposeId(1, 2)),
        ));
        io.send(vec![frame].into_iter()).await.unwrap();
        drop(io);
        drop(storage);

        let mut storage = LogStorage::new("/tmp/wal").await?;
        let mut io = storage.framed_io::<TestCommand>();
        let frame = io.next().await.unwrap().unwrap();
        println!("{:?}", frame[0]);
        Ok(())
    }
}
