#![allow(unused)]

/// The WAL codec
mod codec;

/// File pipeline
mod file_pipeline;

mod util;

/// WAL segment
mod segment;

/// Remover of the segment file
mod remover;

/// WAL tests
#[cfg(test)]
mod tests;

use std::{
    io,
    path::{Path, PathBuf},
    pin::Pin,
    task::Poll,
};

use clippy_utilities::OverflowArithmetic;
use curp_external_api::LogIndex;
use futures::{future::join_all, ready, Future, FutureExt, SinkExt, StreamExt};
use itertools::Itertools;
use serde::{de::DeserializeOwned, Serialize};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_util::codec::Framed;
use tracing::warn;

use crate::log_entry::LogEntry;

use self::{
    codec::{CodecError, DataFrame, WAL},
    file_pipeline::FilePipeline,
    remover::SegmentRemover,
    segment::WALSegment,
    util::LockedFile,
};

const WAL_MAGIC: u32 = 0xd86e0be2;

const WAL_VERSION: u8 = 0x00;

/// Size in bytes per segment, default is 64MiB
const SEGMENT_SIZE_BYTES: u64 = 64 * 1024 * 1024;

const WAL_FILE_EXT: &'static str = ".wal";

type SegmentOpenFut = Pin<Box<dyn Future<Output = io::Result<WALSegment>> + Send>>;

/// Wrapper type of Framed LogStorage
pub(crate) struct FramedWALStorage<C> {
    inner: Framed<WALStorage, WAL<C>>,
}

impl<C> FramedWALStorage<C>
where
    C: Serialize + DeserializeOwned + 'static,
{
    /// Recover from the given directory if there's any segments
    /// Otherwise, creates a new `FramedLogStorage`
    pub(crate) async fn new_or_recover(
        dir: impl AsRef<Path>,
    ) -> io::Result<(Self, Vec<LogEntry<C>>)> {
        let (storage, logs) = WALStorage::new_or_recover::<C>(dir).await?;
        Ok((
            Self {
                inner: Framed::new(storage, WAL::<C>::new()),
            },
            logs,
        ))
    }

    /// Send frames with fsync
    async fn send_sync(&mut self, item: Vec<DataFrame<C>>) -> io::Result<()> {
        if let Some(&DataFrame::Entry(ref entry)) = item.last() {
            self.inner.get_mut().next_log_index = entry.index.overflow_add(1);
        }
        self.inner.send(item).await?;
        self.inner.flush().await?;
        self.inner.get_mut().sync_last_segment().await?;
        Ok(())
    }

    /// Tuncate all the logs whose index is less than or equal to `compact_index`
    ///
    /// `compact_index` should be the smallest index required in CURP
    pub(crate) async fn truncate_head(&mut self, compact_index: LogIndex) -> io::Result<()> {
        self.inner.get_mut().truncate_head::<C>(compact_index).await
    }

    /// Tuncate all the logs whose index is greater than of equal to `next_index`
    pub(crate) async fn truncate_tail(&mut self, next_index: LogIndex) -> io::Result<()> {
        self.inner.get_mut().truncate_tail::<C>(next_index).await
    }
}

/// The log storage
struct WALStorage {
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

impl WALStorage {
    /// Recover from the given directory if there's any segments
    /// Otherwise, creates a new `LogStorage`
    async fn new_or_recover<C>(dir: impl AsRef<Path>) -> io::Result<(Self, Vec<LogEntry<C>>)>
    where
        C: Serialize + DeserializeOwned + 'static,
    {
        // We try to recover the removal first
        SegmentRemover::recover(&dir).await?;

        let mut pipeline = FilePipeline::new(PathBuf::from(dir.as_ref()), SEGMENT_SIZE_BYTES);
        let file_paths = util::get_file_paths_with_ext(&dir, WAL_FILE_EXT)?;
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

        /// If there's no segments to recover, create a new segment
        if segments.is_empty() {
            let Some(lfile) = pipeline.next().await else {
                return Err(io::Error::from(io::ErrorKind::BrokenPipe));
            };
            segments.push(WALSegment::create(lfile, 1, 0).await?);
        }
        let next_segment_id = segments.last().map(|s| s.id().overflow_add(1)).unwrap_or(0);
        let next_log_index = logs_flattened
            .last()
            .map(|l| l.index.overflow_add(1))
            .unwrap_or(1);

        Ok((
            Self {
                dir: PathBuf::from(dir.as_ref()),
                pipeline,
                segments,
                next_segment_id,
                next_log_index,
                segment_opening: None,
            },
            logs_flattened,
        ))
    }

    /// Tuncate all the logs whose index is less than or equal to `compact_index`
    ///
    /// `compact_index` should be the smallest index required in CURP
    async fn truncate_head<C>(&mut self, compact_index: LogIndex) -> io::Result<()>
    where
        C: Serialize,
    {
        if compact_index >= self.next_log_index {
            warn!(
                "head truncation: compact index too large, compact index: {}, storage next index: {}",
                compact_index, self.next_log_index
            );
            return Ok(());
        }

        let segments: Vec<_> = self
            .segments
            .iter()
            .take_while(|s| s.base_index() <= compact_index)
            .collect();

        if segments.is_empty() {
            return Ok(());
        }

        // The last segment does not need to be removed
        let to_remove = segments.into_iter().rev().skip(1);
        SegmentRemover::new_removal(&self.dir, to_remove).await?;

        Ok(())
    }

    /// Tuncate all the logs whose index is greater than of equal to `next_index`
    async fn truncate_tail<C>(&mut self, next_index: LogIndex) -> io::Result<()>
    where
        C: Serialize,
    {
        // TODO: Make sure that all writing operations are completed, maybe guaranteed by type system?
        assert!(
            self.segment_opening.is_none(),
            "There's a inflight segment file opening"
        );

        // segments to truncate
        let segments = self
            .segments
            .iter()
            .rev()
            .take_while_inclusive::<_>(|s| s.base_index() > next_index);

        for segment in segments {
            segment.seal::<C>(next_index).await;
        }

        let to_remove = self.update_segments().await;
        SegmentRemover::new_removal(&self.dir, to_remove.iter()).await?;

        self.next_log_index = next_index;
        self.open_new_segment().await?;

        Ok(())
    }

    /// Open a new WAL segment
    async fn open_new_segment(&mut self) -> io::Result<()> {
        let lfile = self
            .pipeline
            .next()
            .await
            .ok_or(io::Error::from(io::ErrorKind::BrokenPipe))?;

        let segment = WALSegment::create(lfile, self.next_log_index, self.next_segment_id).await?;

        self.segments.push(segment);
        self.next_segment_id = self.next_segment_id.overflow_add(1);

        Ok(())
    }

    /// Remove segments that is no long needed
    async fn update_segments(&mut self) -> Vec<WALSegment> {
        let flags = join_all(self.segments.iter().map(|s| s.is_redundant())).await;
        let (to_remove, remaining): (Vec<_>, Vec<_>) = self
            .segments
            .drain(..)
            .zip(flags.into_iter())
            .partition(|(_, f)| *f);

        self.segments = remaining.into_iter().map(|(s, _)| s).collect();

        to_remove.into_iter().map(|(s, _)| s).collect()
    }

    async fn sync_last_segment(&mut self) -> io::Result<()> {
        let segment = self.last_segment().await?;
        segment.sync_all().await
    }

    /// Recover log entries from a `WALSegment`
    async fn recover_segment_logs<C>(
        segment: WALSegment,
    ) -> io::Result<impl Iterator<Item = LogEntry<C>>>
    where
        C: Serialize + DeserializeOwned + 'static,
    {
        let mut framed = Framed::new(segment, WAL::<C>::new());
        let mut frame_batches = vec![];
        while let Some(result) = framed.next().await {
            match result {
                Ok(f) => frame_batches.push(f),
                Err(e) => {
                    /// If the segment file reaches on end, stop reading
                    if matches!(e, CodecError::EndOrCorrupted) {
                        break;
                    } else {
                        println!("{e}");
                        return Err(io::Error::from(io::ErrorKind::InvalidData));
                    }
                }
            }
        }
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

        // Update seal index
        framed.get_mut().update_seal_index(highest_index).await;

        // Get log entries that index is no larger than `highest_index`
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

    /// Convert self into a Framed instance
    fn into_framed<C>(self) -> Framed<Self, WAL<C>> {
        Framed::new(self, WAL::<C>::new())
    }

    fn last_segment(&mut self) -> LastSegmentFut<'_> {
        LastSegmentFut { storage: self }
    }
}

impl AsyncWrite for WALStorage {
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

impl AsyncRead for WALStorage {
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

impl WALStorage {
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
        if ready!(Box::pin(last.len()).poll_unpin(cx)) < SEGMENT_SIZE_BYTES {
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
    storage: &'a mut WALStorage,
}

impl Future for LastSegmentFut<'_> {
    type Output = io::Result<WALSegment>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut *self.storage).poll_last_segment(cx)
    }
}
