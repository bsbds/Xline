#![allow(unused, dead_code)]

mod codec;

/// File pipeline
mod file_pipeline;

mod util;

/// WAL segment
mod segment;

use std::{io, path::PathBuf, pin::Pin, sync::Arc, task::Poll};

use async_trait::async_trait;
use clippy_utilities::OverflowArithmetic;
use curp_external_api::LogIndex;
use futures::{future::join_all, ready, Future, FutureExt, StreamExt};
use serde::Serialize;
use tokio::{
    io::{AsyncRead, AsyncWrite},
    sync::Mutex,
};
use tokio_util::codec::Framed;

use self::{codec::WAL, file_pipeline::FilePipeline, segment::WALSegment, util::LockedFile};

const WAL_MAGIC: u32 = 0xd86e0be2;

const WAL_VERSION: u8 = 0x00;

/// Size in bytes per segment, default is 64MiB
const SEGMENT_SIZE_BYTES: u64 = 64 * 1024 * 1024;

const WAL_FILE_EXT: &'static str = ".wal";

type SegmentOpenFut = Pin<Box<dyn Future<Output = io::Result<WALSegment>> + Send>>;

#[async_trait]
pub(super) trait Fsync {
    /// Call fsync on the storage
    async fn sync_all(&mut self) -> io::Result<()>;
}

/// The log storage
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
    /// Future of an opening segment
    segment_opening: Option<SegmentOpenFut>,
}

impl LogStorage {
    /// Creates a new `LogStorage`
    pub(crate) async fn new(dir: &str) -> io::Result<Self> {
        let dir = PathBuf::from(dir);
        let mut pipeline = FilePipeline::new(dir.clone(), SEGMENT_SIZE_BYTES);
        let Some(lfile) = pipeline.next().await else {
            return Err(io::Error::from(io::ErrorKind::BrokenPipe));
        };
        let initial_log_index = 1;
        let initial_segment_id = 0;
        let segment = WALSegment::create(lfile, initial_log_index, initial_segment_id).await?;

        Ok(Self {
            dir,
            pipeline,
            segments: vec![segment],
            next_segment_id: initial_segment_id.overflow_add(1),
            next_log_index: initial_segment_id.overflow_add(1),
            segment_opening: None,
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
            segment_opening: None,
        })
    }

    fn into_framed<C: Serialize>(self) -> Framed<Self, WAL<C>> {
        Framed::new(self, WAL::<C>::new())
    }

    fn last_segment(&mut self) -> LastSegmentFut<'_> {
        LastSegmentFut { storage: self }
    }
}

impl AsyncWrite for LogStorage {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, io::Error>> {
        let mut segment = match ready!(self.poll_last_segment(cx)) {
            Ok(segment) => segment,
            Err(e) => return Poll::Ready(Err(e)),
        };
        Pin::new(&mut segment).poll_write(cx, buf)
    }

    fn poll_flush(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Result<(), io::Error>> {
        let mut segment = match ready!(self.poll_last_segment(cx)) {
            Ok(segment) => segment,
            Err(e) => return Poll::Ready(Err(e)),
        };
        Pin::new(&mut segment).poll_flush(cx)
    }

    fn poll_shutdown(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Result<(), io::Error>> {
        let mut segment = match ready!(self.poll_last_segment(cx)) {
            Ok(segment) => segment,
            Err(e) => return Poll::Ready(Err(e)),
        };
        Pin::new(&mut segment).poll_shutdown(cx)
    }
}

impl AsyncRead for LogStorage {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let mut segment = match ready!(self.poll_last_segment(cx)) {
            Ok(segment) => segment,
            Err(e) => return Poll::Ready(Err(e)),
        };
        Pin::new(&mut segment).poll_read(cx, buf)
    }
}

#[async_trait]
impl Fsync for LogStorage {
    async fn sync_all(&mut self) -> io::Result<()> {
        let segment = self.last_segment().await?;
        segment.sync_all().await
    }
}

impl LogStorage {
    fn poll_last_segment(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<io::Result<WALSegment>> {
        if let Some(ref mut fut) = self.segment_opening {
            return match ready!(fut.poll_unpin(cx)) {
                Ok(segment) => {
                    self.segments.push(segment.clone());
                    Poll::Ready(Ok(segment))
                }
                Err(e) => Poll::Ready(Err(e)),
            };
        }
        let last = self
            .segments
            .last()
            .unwrap_or_else(|| unreachable!("there should exist at least one segement"))
            .clone();
        if last.len() < SEGMENT_SIZE_BYTES {
            Poll::Ready(Ok(last))
        } else {
            // We open a new segment if the segment reaches the soft limit
            let fut = match ready!(self.pipeline.poll_next_unpin(cx)) {
                Some(lfile) => WALSegment::create(lfile, self.next_log_index, self.next_segment_id),
                None => return Poll::Ready(Err(io::Error::from(io::ErrorKind::BrokenPipe))),
            };
            self.segment_opening = Some(Box::pin(fut));
            Poll::Pending
        }
    }
}

struct LastSegmentFut<'a> {
    storage: &'a mut LogStorage,
}

impl Future for LastSegmentFut<'_> {
    type Output = io::Result<WALSegment>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut *self.storage).poll_last_segment(cx)
    }
}

#[async_trait]
impl<T, U> Fsync for Framed<T, U>
where
    T: Fsync + Send,
    U: Send,
{
    async fn sync_all(&mut self) -> io::Result<()> {
        let storage = self.get_mut();
        storage.sync_all().await
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
        let mut io = storage.into_framed();
        let frame = DataFrame::Entry(LogEntry::<TestCommand>::new(
            1,
            1,
            EntryData::Empty(ProposeId(1, 2)),
        ));
        io.send(vec![frame].into_iter()).await.unwrap();
        drop(io);

        let mut storage = LogStorage::new("/tmp/wal").await?;
        let mut io = storage.into_framed::<TestCommand>();
        let frame = io.next().await.unwrap().unwrap();
        println!("{:?}", frame[0]);
        Ok(())
    }
}
