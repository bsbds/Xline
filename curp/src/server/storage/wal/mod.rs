mod codec;

/// File pipeline
mod file_pipeline;

mod util;

use std::{io, path::PathBuf};

use curp_external_api::LogIndex;
use serde::Serialize;
use tokio::fs::File as TokioFile;
use tokio_stream::StreamExt;
use tokio_util::codec::Framed;

use self::{codec::WAL, file_pipeline::FilePipeline, util::LockedFile};

/// Size in bytes per segment, default is 64MiB
const SEGMENT_SIZE_BYTES: u64 = 64 * 1024 * 1024;

const WAL_FILE_EXT: &'static str = ".wal";

/// The log storage
#[derive(Debug)]
pub(super) struct LogStorage {
    /// The directory to store the log files
    dir: PathBuf,
    /// The pipeline that pre-allocates files
    pipeline: FilePipeline,
    /// Locked files
    lfiles: Vec<LockedFile>,
    /// Next segment id
    next_segment_id: u64,
    /// Next segment id
    next_log_index: LogIndex,
}

impl LogStorage {
    /// Creates a new `LogStorage`
    pub(crate) async fn new(dir: &str) -> io::Result<Self> {
        let dir = PathBuf::from(dir);
        let pipeline = FilePipeline::new(dir.clone(), SEGMENT_SIZE_BYTES);

        Ok(Self {
            dir,
            pipeline,
            lfiles: vec![],
            next_segment_id: 0,
            next_log_index: 1,
        })
    }

    pub(crate) async fn recover(dir: PathBuf) -> io::Result<Self> {
        let file_paths = util::get_file_paths_with_ext(dir.clone(), WAL_FILE_EXT)?;
        let lfiles = file_paths
            .into_iter()
            .map(|p| LockedFile::open_read_append(p))
            .collect::<io::Result<_>>()?;
        let pipeline = FilePipeline::new(dir.clone(), SEGMENT_SIZE_BYTES);

        // TODO: recover segment info from header
        Ok(Self {
            dir,
            pipeline,
            lfiles,
            next_segment_id: 0,
            next_log_index: 1,
        })
    }

    /// Open a new wal segment file
    pub(crate) async fn open_new_segment(&mut self) -> io::Result<()> {
        let Some(tmp_file) = self.pipeline.next().await else { return Err(io::Error::from(io::ErrorKind::Other)) };
        let segment_name = Self::segment_name(self.next_segment_id, self.next_log_index);
        let lfile = tmp_file.rename(segment_name)?;
        self.lfiles.push(lfile);
        Ok(())
    }

    pub(crate) fn framed_io<C: Serialize>(&mut self) -> Framed<TokioFile, WAL<C>> {
        let file = self.lfiles.pop().unwrap();
        Framed::new(file.into_async(), WAL::<C>::new())
    }

    /// Gets the file name of the WAL segment
    fn segment_name(segment_id: u64, log_index: u64) -> String {
        format!("{:016x}-{:016x}", segment_id, log_index)
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
