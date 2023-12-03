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

/// The config for WALStorage
mod config;

/// WAL tests
#[cfg(test)]
mod tests;

use std::{
    collections::{HashMap, HashSet, VecDeque},
    io,
    marker::PhantomData,
    path::{Path, PathBuf},
    pin::Pin,
    task::Poll,
};

use clippy_utilities::OverflowArithmetic;
use curp_external_api::LogIndex;
use futures::{future::join_all, ready, Future, FutureExt, SinkExt, StreamExt};
use itertools::Itertools;
use serde::{de::DeserializeOwned, Serialize};
use thiserror::Error;
use tokio::{
    io::{AsyncRead, AsyncWrite},
    macros::support::poll_fn,
};
use tokio_util::codec::Framed;
use tracing::warn;

use crate::log_entry::LogEntry;

use self::{
    codec::{CodecError, DataFrame, WAL},
    config::WALConfig,
    file_pipeline::FilePipeline,
    remover::SegmentRemover,
    segment::{IOState, WALSegment},
    util::LockedFile,
};

const WAL_MAGIC: u32 = 0xd86e0be2;

const WAL_VERSION: u8 = 0x00;

const WAL_FILE_EXT: &'static str = ".wal";

#[derive(Debug, Error)]
pub(crate) enum WALStorageError {
    #[error("WAL corrupted")]
    Corrupted,
    #[error("Codec error: {0}")]
    Codec(#[from] CodecError),
    #[error("IO error: {0}")]
    IO(#[from] io::Error),
}

/// Wrapper type of Framed LogStorage
///
/// This exists beacuse we don't want to leak the implementation of the AsyncRead/AsyncWrite trait
pub(crate) struct WALStorage<C> {
    /// The inner storage type
    inner: Inner<C>,
}

impl<C> WALStorage<C>
where
    C: Serialize + DeserializeOwned + Unpin + 'static,
{
    /// Recover from the given directory if there's any segments
    /// Otherwise, creates a new `FramedLogStorage`
    pub(crate) async fn new_or_recover(
        config: WALConfig,
    ) -> Result<(Self, Vec<LogEntry<C>>), WALStorageError> {
        let (storage, logs) = Inner::new_or_recover(config).await?;
        Ok((Self { inner: storage }, logs))
    }

    /// Send frames with fsync
    async fn send_sync(&mut self, item: Vec<DataFrame<C>>) -> io::Result<()> {
        self.inner.send_sync(item).await
    }

    /// Tuncate all the logs whose index is less than or equal to `compact_index`
    ///
    /// `compact_index` should be the smallest index required in CURP
    pub(crate) async fn truncate_head(&mut self, compact_index: LogIndex) -> io::Result<()> {
        self.inner.truncate_head(compact_index).await
    }

    /// Tuncate all the logs whose index is greater than `max_index`
    pub(crate) async fn truncate_tail(&mut self, max_index: LogIndex) -> io::Result<()> {
        self.inner.truncate_tail(max_index).await
    }
}

type SegmentOpenFut = Pin<Box<dyn Future<Output = io::Result<WALSegment>> + Send>>;

/// The log storage
struct Inner<C> {
    /// The directory to store the log files
    config: WALConfig,
    /// The pipeline that pre-allocates files
    pipeline: FilePipeline,
    /// WAL segements
    segments: Vec<WALSegment>,
    /// Next segment id
    next_segment_id: u64,
    /// Next segment id
    next_log_index: LogIndex,
    /// Future of an opening segment
    segment_opening_fut: Option<SegmentOpenFut>,

    /// The phantom data
    _phantom: PhantomData<C>,
}

impl<C> Inner<C>
where
    C: Serialize + DeserializeOwned + Unpin + 'static,
{
    /// Recover from the given directory if there's any segments
    /// Otherwise, creates a new `LogStorage`
    async fn new_or_recover(
        config: WALConfig,
    ) -> Result<(Self, Vec<LogEntry<C>>), WALStorageError> {
        // We try to recover the removal first
        SegmentRemover::recover(&config.dir).await?;

        let mut pipeline = FilePipeline::new(config.dir.clone(), config.max_segment_size);
        let file_paths = util::get_file_paths_with_ext(&config.dir, WAL_FILE_EXT)?;
        let lfiles: Vec<_> = file_paths
            .into_iter()
            .map(|p| LockedFile::open_rw(p))
            .collect::<io::Result<_>>()?;

        let segment_futs = lfiles.into_iter().map(|f| WALSegment::open(f));
        let mut segments: Vec<_> = join_all(segment_futs)
            .await
            .into_iter()
            .collect::<io::Result<_>>()?;
        segments.sort_unstable();

        let logs_fut: Vec<_> = segments
            .iter_mut()
            .map(Self::recover_segment_logs)
            .collect();
        let logs: Vec<_> = join_all(logs_fut)
            .await
            .into_iter()
            .collect::<Result<_, _>>()?;
        let logs_flattened: Vec<_> = logs.into_iter().flatten().collect();

        if !Self::check_log_continuity(logs_flattened.iter()) {
            for logs in &logs_flattened {
                println!("index: {}", logs.index);
            }
            return Err(WALStorageError::Corrupted);
        }

        /// If there's no segments to recover, create a new segment
        if segments.is_empty() {
            let lfile = pipeline
                .next()
                .await
                .ok_or(io::Error::from(io::ErrorKind::BrokenPipe))??;
            segments.push(WALSegment::create(lfile, 1, 0).await?);
        }
        let next_segment_id = segments.last().map(|s| s.id().overflow_add(1)).unwrap_or(0);
        let next_log_index = logs_flattened
            .last()
            .map(|l| l.index.overflow_add(1))
            .unwrap_or(1);

        Ok((
            Self {
                config,
                pipeline,
                segments,
                next_segment_id,
                next_log_index,
                segment_opening_fut: None,
                _phantom: PhantomData,
            },
            logs_flattened,
        ))
    }

    /// Send frames with fsync
    async fn send_sync(&mut self, item: Vec<DataFrame<C>>) -> io::Result<()> {
        let mut self_framed = Framed::new(self, WAL::<C>::new());
        if let Some(&DataFrame::Entry(ref entry)) = item.last() {
            self_framed.get_mut().next_log_index = entry.index.overflow_add(1);
        }
        self_framed.send(item).await?;
        self_framed.flush().await?;
        self_framed.get_mut().sync_segments().await?;
        Ok(())
    }

    /// Tuncate all the logs whose index is less than or equal to `compact_index`
    ///
    /// `compact_index` should be the smallest index required in CURP
    async fn truncate_head(&mut self, compact_index: LogIndex) -> io::Result<()> {
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
        SegmentRemover::new_removal(&self.config.dir, to_remove).await?;

        Ok(())
    }

    /// Tuncate all the logs whose index is greater than `max_index`
    async fn truncate_tail(&mut self, max_index: LogIndex) -> io::Result<()> {
        // segments to truncate
        let segments = self
            .segments
            .as_mut_slice()
            .into_iter()
            .rev()
            .take_while_inclusive::<_>(|s| s.base_index() > max_index);

        for segment in segments {
            segment.seal::<C>(max_index).await;
        }

        let to_remove = self.update_segments().await;
        SegmentRemover::new_removal(&self.config.dir, to_remove.iter()).await?;

        self.next_log_index = max_index.overflow_add(1);
        self.open_new_segment().await?;

        Ok(())
    }

    /// Open a new WAL segment
    async fn open_new_segment(&mut self) -> io::Result<()> {
        let lfile = self
            .pipeline
            .next()
            .await
            .ok_or(io::Error::from(io::ErrorKind::BrokenPipe))??;

        let segment = WALSegment::create(lfile, self.next_log_index, self.next_segment_id).await?;

        self.segments.push(segment);
        self.next_segment_id = self.next_segment_id.overflow_add(1);

        Ok(())
    }

    /// Remove segments that is no long needed
    async fn update_segments(&mut self) -> Vec<WALSegment> {
        let flags: Vec<_> = self.segments.iter().map(|s| s.is_redundant()).collect();
        let (to_remove, remaining): (Vec<_>, Vec<_>) = self
            .segments
            .drain(..)
            .zip(flags.into_iter())
            .partition(|(_, f)| *f);

        self.segments = remaining.into_iter().map(|(s, _)| s).collect();

        to_remove.into_iter().map(|(s, _)| s).collect()
    }

    /// Sync all flushed segments
    async fn sync_segments(&mut self) -> io::Result<()> {
        let to_sync = self
            .segments
            .as_mut_slice()
            .into_iter()
            .filter(|s| matches!(s.state(), IOState::Flushed));
        for segment in to_sync {
            segment.sync_all().await?;
        }

        Ok(())
    }

    /// Recover log entries from a `WALSegment`
    async fn recover_segment_logs(
        segment: &mut WALSegment,
    ) -> Result<impl Iterator<Item = LogEntry<C>>, WALStorageError> {
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
                        return Err(e.into());
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
        framed.get_mut().update_seal_index(highest_index);

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
    fn check_log_continuity<'a>(entries: impl Iterator<Item = &'a LogEntry<C>> + Clone) -> bool {
        entries
            .clone()
            .zip(entries.skip(1))
            .all(|(x, y)| x.index.overflow_add(1) == y.index)
    }

    fn last_segment(&mut self) -> LastSegmentFut<'_, C> {
        LastSegmentFut {
            storage: Some(self),
        }
    }
}

impl<C> Inner<C> {
    fn poll_last_segment(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<io::Result<&mut WALSegment>> {
        if let Some(ref mut fut) = self.segment_opening_fut {
            return match ready!(fut.poll_unpin(cx)) {
                Ok(segment) => {
                    self.segment_opening_fut = None;
                    self.segments.push(segment);
                    Poll::Ready(Ok(self
                        .segments
                        .last_mut()
                        .unwrap_or_else(|| unreachable!())))
                }
                Err(e) => Poll::Ready(Err(e)),
            };
        }

        let last = self
            .segments
            .last_mut()
            .unwrap_or_else(|| unreachable!("there should exist at least one segement"));
        if last.size() < self.config.max_segment_size {
            Poll::Ready(Ok(self.segments.last_mut().unwrap()))
        } else {
            // We open a new segment if the segment reaches the soft limit
            let mut create_fut = Box::pin(match ready!(self.pipeline.poll_next_unpin(cx)) {
                Some(Ok(lfile)) => {
                    WALSegment::create(lfile, self.next_log_index, self.next_segment_id)
                }
                Some(Err(e)) => return Poll::Ready(Err(e)),
                None => return Poll::Ready(Err(io::Error::from(io::ErrorKind::BrokenPipe))),
            });

            match create_fut.poll_unpin(cx) {
                Poll::Ready(Ok(res)) => {
                    self.segments.push(res);
                    Poll::Ready(Ok(self
                        .segments
                        .last_mut()
                        .unwrap_or_else(|| unreachable!())))
                }
                Poll::Ready(Err(e)) => Poll::Ready(Err(e)),
                Poll::Pending => {
                    self.segment_opening_fut = Some(create_fut);
                    Poll::Pending
                }
            }
        }
    }
}

impl<C> AsyncWrite for Inner<C>
where
    C: Unpin,
{
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, io::Error>> {
        let mut segment = match ready!(self.get_mut().poll_last_segment(cx)) {
            Ok(segment) => segment,
            Err(e) => return Poll::Ready(Err(e)),
        };
        Pin::new(&mut segment).poll_write(cx, buf)
    }

    fn poll_flush(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Result<(), io::Error>> {
        let mut to_flush = self
            .segments
            .as_mut_slice()
            .into_iter()
            .filter(|s| matches!(s.state(), IOState::Written));

        if to_flush.all(|s| Pin::new(s).poll_flush(cx).is_ready()) {
            Poll::Ready(Ok(()))
        } else {
            Poll::Pending
        }
    }

    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Result<(), io::Error>> {
        if self
            .segments
            .as_mut_slice()
            .into_iter()
            .all(|s| Pin::new(s).poll_shutdown(cx).is_ready())
        {
            Poll::Ready(Ok(()))
        } else {
            Poll::Pending
        }
    }
}

impl<C> AsyncRead for Inner<C> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        unimplemented!("AsyncRead for WALStorage not implemented yet")
    }
}

/// The future that will resolv to the last segment of the storage
struct LastSegmentFut<'a, C> {
    /// The reference to the storage
    storage: Option<&'a mut Inner<C>>,
}

impl<'a, C> Future for LastSegmentFut<'a, C> {
    type Output = io::Result<&'a mut WALSegment>;

    #[allow(clippy::unwrap_used)] // `storage` should always contains a value
    fn poll(mut self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Self::Output> {
        /// This is to satisfy the borrow checker
        let _ignore = ready!(self.storage.as_mut().unwrap().poll_last_segment(cx));
        self.storage.take().unwrap().poll_last_segment(cx)
    }
}
