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
use serde::{de::DeserializeOwned, Serialize};
use tokio::{
    io::{AsyncRead, AsyncWrite},
    sync::Mutex,
};
use tokio_util::codec::Framed;

use crate::log_entry::LogEntry;

use self::{
    codec::{DataFrame, WAL},
    file_pipeline::FilePipeline,
    segment::WALSegment,
    util::LockedFile,
};

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

    /// Recover from the given directory
    pub(crate) async fn recover<C>(dir: PathBuf) -> io::Result<Self>
    where
        C: Serialize + DeserializeOwned + 'static,
    {
        let pipeline = FilePipeline::new(dir.clone(), SEGMENT_SIZE_BYTES);
        let file_paths = util::get_file_paths_with_ext(dir.clone(), WAL_FILE_EXT)?;
        let lfiles: Vec<_> = file_paths
            .into_iter()
            .map(|p| LockedFile::open_read_append(p))
            .collect::<io::Result<_>>()?;

        let segment_futs = lfiles.into_iter().map(|f| WALSegment::open(f));
        let mut segments: Vec<_> = join_all(segment_futs)
            .await
            .into_iter()
            .collect::<io::Result<_>>()?;
        segments.sort_unstable();

        let logs_fut: Vec<_> = segments
            .clone()
            .into_iter()
            .map(Self::recover_segment_logs::<C>)
            .collect();
        let logs: Vec<_> = join_all(logs_fut)
            .await
            .into_iter()
            .collect::<Result<_, _>>()?;
        let logs_flattened: Vec<_> = logs.into_iter().flatten().collect();

        if !Self::check_log_continuity(logs_flattened.iter()) {
            return Err(io::Error::from(io::ErrorKind::InvalidData));
        }

        // TODO: seal the last segment and create a new one
        let next_segment_id = segments.last().map(|s| s.id().overflow_add(1)).unwrap_or(0);
        let next_log_index = logs_flattened
            .last()
            .map(|l| l.index.overflow_add(1))
            .unwrap_or(1);

        Ok(Self {
            dir,
            pipeline,
            segments,
            next_segment_id,
            next_log_index,
            segment_opening: None,
        })
    }

    /// Recover log entries from a `WALSegment`
    async fn recover_segment_logs<C>(
        segment: WALSegment,
    ) -> io::Result<impl Iterator<Item = LogEntry<C>>>
    where
        C: Serialize + DeserializeOwned + 'static,
    {
        let mut framed = Framed::new(segment, WAL::<C>::new());
        let mut result = vec![];
        while let Some(r) = framed.next().await {
            result.push(r);
        }
        let frame_batches: Vec<_> = result.into_iter().collect::<Result<_, _>>()?;
        // The highest_index of this segment
        let mut highest_index = u64::MAX;
        // We get the last frame batch to check it's type
        match frame_batches.last() {
            Some(frames) => {
                let frame = frames
                    .last()
                    .unwrap_or_else(|| unreachable!("a batch should contains at leat one frame"));
                if let DataFrame::SealIndex(index) = *frame {
                    highest_index = index;
                }
            }
            // The segment does not contains any frame, only a file header
            None => {
                todo!("handle no frame")
            }
        }

        // TODO: reset on drop might be better
        framed.get_mut().reset_offset().await?;

        /// Get log entries that index is no larger than `highest_index`
        Ok(frame_batches.into_iter().flatten().filter_map(move |f| {
            if let DataFrame::Entry(e) = f {
                (e.index <= highest_index).then_some(e)
            } else {
                None
            }
        }))
    }

    /// Check if the log index are continuous
    #[allow(single_use_lifetimes)] // the lifetime is required here
    fn check_log_continuity<'a, C>(entries: impl Iterator<Item = &'a LogEntry<C>> + Clone) -> bool
    where
        C: 'static,
    {
        entries
            .clone()
            .zip(entries.skip(1))
            .all(|(x, y)| x.index.overflow_add(1) == y.index)
    }

    fn into_framed<C>(self) -> Framed<Self, WAL<C>>
    where
        C: Serialize + DeserializeOwned + 'static,
    {
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
