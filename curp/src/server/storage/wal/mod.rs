mod codec;

/// File pipeline
mod file_pipeline;

mod util;

use std::{io, path::PathBuf};

use curp_external_api::LogIndex;
use futures::future::join_all;
use serde::Serialize;
use tokio::{
    fs::File as TokioFile,
    io::{AsyncReadExt, AsyncWriteExt},
};
use tokio_stream::StreamExt;
use tokio_util::codec::Framed;

use self::{
    codec::WAL,
    file_pipeline::FilePipeline,
    util::{get_checksum, validate_data, LockedFile},
};

const WAL_MAGIC: u32 = 0xd86e0be2;

const WAL_VERSION: u8 = 0x00;

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
    /// WAL segements
    segments: Vec<WALSegment>,
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
            segments: vec![],
            next_segment_id: 0,
            next_log_index: 1,
        })
    }

    pub(crate) async fn recover(dir: PathBuf) -> io::Result<Self> {
        let pipeline = FilePipeline::new(dir.clone(), SEGMENT_SIZE_BYTES);
        let file_paths = util::get_file_paths_with_ext(dir.clone(), WAL_FILE_EXT)?;
        let lfiles: Vec<_> = file_paths
            .into_iter()
            .map(|p| LockedFile::open_read_append(p))
            .collect::<io::Result<_>>()?;

        // TODO: recover segment info from header
        let segment_futs = lfiles.into_iter().map(|f| WALSegment::open(f));
        let segments = join_all(segment_futs)
            .await
            .into_iter()
            .collect::<io::Result<_>>()?;

        Ok(Self {
            dir,
            pipeline,
            segments,
            next_segment_id: 0,
            next_log_index: 1,
        })
    }

    /// Open a new wal segment file
    pub(crate) async fn open_new_segment(&mut self) -> io::Result<()> {
        let Some(tmp_file) = self.pipeline.next().await else { return Err(io::Error::from(io::ErrorKind::Other)) };
        let segment =
            WALSegment::create(tmp_file, self.next_log_index, self.next_segment_id).await?;
        self.segments.push(segment);
        Ok(())
    }

    pub(crate) fn framed_io<C: Serialize>(&mut self) -> Framed<TokioFile, WAL<C>> {
        let segment = self.segments.pop().unwrap();
        Framed::new(segment.into_inner(), WAL::<C>::new())
    }
}

#[derive(Debug)]
struct WALSegment {
    file: TokioFile,
    base_index: LogIndex,
    segment_id: u64,
}

impl WALSegment {
    /// Open an existing WAL segment file
    async fn open(lfile: LockedFile) -> io::Result<Self> {
        let mut file = lfile.into_async();
        let mut buf = vec![0; 56];
        file.read_exact(&mut buf).await;
        let (base_index, segment_id) = Self::parse_header(&buf)?;

        Ok(Self {
            file,
            base_index,
            segment_id,
        })
    }

    /// Creates a new `WALSegment`
    async fn create(
        tmp_file: LockedFile,
        base_index: LogIndex,
        segment_id: u64,
    ) -> io::Result<Self> {
        let segment_name = Self::segment_name(segment_id, base_index);
        let lfile = tmp_file.rename(segment_name)?;
        let mut file = lfile.into_async();
        file.write_all(&Self::gen_header(base_index, segment_id))
            .await?;

        Ok(Self {
            file,
            base_index,
            segment_id,
        })
    }

    /// Convert this into the inner tokio file
    fn into_inner(self) -> TokioFile {
        self.file
    }

    /// Gets the file name of the WAL segment
    fn segment_name(segment_id: u64, log_index: u64) -> String {
        format!("{:016x}-{:016x}", segment_id, log_index)
    }

    // Generate the header
    //
    // The header layout:
    //
    // 0      1      2      3      4      5      6      7      8
    // +------+------+------+------+------+------+------+------+
    // | Magic                     | Reserved           | Vsn  |
    // +------+------+------+------+------+------+------+------+
    // | BaseIndex                                             |
    // +------+------+------+------+------+------+------+------+
    // | SegmentID                                             |
    // +------+------+------+------+------+------+------+------+
    // | Checksum (32bytes)                                    |
    // +                                                       +
    // |                                                       |
    // +                                                       +
    // |                                                       |
    // +                                                       +
    // |                                                       |
    // +------+------+------+------+------+------+------+------+
    fn gen_header(base_index: LogIndex, segment_id: u64) -> Vec<u8> {
        let mut buf = vec![];
        buf.extend(WAL_MAGIC.to_le_bytes());
        buf.extend(vec![0; 7]);
        buf.push(WAL_VERSION);
        buf.extend(base_index.to_le_bytes());
        buf.extend(segment_id.to_le_bytes());
        buf.extend(get_checksum(&buf));
        buf
    }

    /// Parse the header from the given buffer
    #[allow(clippy::unwrap_used)] // unwrap is used to convert slice to const length and is safe
    fn parse_header(src: &[u8]) -> io::Result<(LogIndex, u64)> {
        let mut offset = 0;
        let mut next_field = |len: usize| {
            offset += len;
            &src[(offset - len)..offset]
        };
        let parse_error = Err(io::Error::from(io::ErrorKind::InvalidData));
        if src.len() < 56 {
            return parse_error;
        }
        if next_field(4) != WAL_MAGIC.to_le_bytes()
            || next_field(3) != &[0; 7]
            || next_field(1) != &[WAL_VERSION]
        {
            return parse_error;
        }
        let base_index = u64::from_le_bytes(next_field(8).try_into().unwrap());
        let segment_id = u64::from_le_bytes(next_field(8).try_into().unwrap());
        let checksum = next_field(32);

        // TODO: better return custom error
        if !validate_data(&src[0..24], checksum) {
            return parse_error;
        }

        Ok((base_index, segment_id))
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
