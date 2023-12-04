#![allow(unused)]

/// WAL errors
mod error;

/// The WAL codec
mod codec;

/// File pipeline
mod file_pipeline;

/// File utils
mod util;

/// WAL segment
mod segment;

/// Remover of the segment file
mod remover;

/// The config for `WALStorage`
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
    codec::{DataFrame, WAL},
    config::WALConfig,
    error::{CorruptType, WALError},
    file_pipeline::FilePipeline,
    remover::SegmentRemover,
    segment::{IOState, WALSegment},
    util::LockedFile,
};

/// The magic of the WAL file
const WAL_MAGIC: u32 = 0xd86e_0be2;

/// The current WAL version
const WAL_VERSION: u8 = 0x00;

/// The wal file extension
const WAL_FILE_EXT: &str = ".wal";

/// The WAL storage
struct WALStorage<C> {
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
    /// The phantom data
    _phantom: PhantomData<C>,
}

impl<C> WALStorage<C>
where
    C: Serialize + DeserializeOwned + Unpin + 'static,
{
    /// Recover from the given directory if there's any segments
    /// Otherwise, creates a new `LogStorage`
    pub(super) async fn new_or_recover(
        config: WALConfig,
    ) -> Result<(Self, Vec<LogEntry<C>>), WALError> {
        // We try to recover the removal first
        SegmentRemover::recover(&config.dir).await?;

        let mut pipeline = FilePipeline::new(config.dir.clone(), config.max_segment_size);
        let file_paths = util::get_file_paths_with_ext(&config.dir, WAL_FILE_EXT)?;
        let lfiles: Vec<_> = file_paths
            .into_iter()
            .map(LockedFile::open_rw)
            .collect::<io::Result<_>>()?;

        let segment_futs = lfiles
            .into_iter()
            .map(|f| WALSegment::open(f, config.max_segment_size));
        let mut segments: Vec<_> = join_all(segment_futs)
            .await
            .into_iter()
            .collect::<Result<_, _>>()?;
        segments.sort_unstable();

        let logs_fut: Vec<_> = segments
            .iter_mut()
            .map(WALSegment::recover_segment_logs)
            .collect();
        let logs: Vec<_> = join_all(logs_fut)
            .await
            .into_iter()
            .collect::<Result<_, _>>()?;
        let logs_flattened: Vec<_> = logs.into_iter().flatten().collect();

        if !Self::check_log_continuity(logs_flattened.iter()) {
            return Err(WALError::Corrupted(CorruptType::LogNotContinue));
        }

        /// If there's no segments to recover, create a new segment
        if segments.is_empty() {
            let lfile = pipeline
                .next()
                .await
                .ok_or(io::Error::from(io::ErrorKind::BrokenPipe))??;
            segments.push(WALSegment::create(lfile, 1, 0, config.max_segment_size).await?);
        }
        let next_segment_id = segments.last().map_or(0, |s| s.id().overflow_add(1));
        let next_log_index = logs_flattened.last().map_or(1, |l| l.index.overflow_add(1));

        Ok((
            Self {
                config,
                pipeline,
                segments,
                next_segment_id,
                next_log_index,
                _phantom: PhantomData,
            },
            logs_flattened,
        ))
    }

    /// Send frames with fsync
    #[allow(clippy::pattern_type_mismatch)] // Cannot satisfiy both clippy
    pub(super) async fn send_sync(&mut self, item: Vec<DataFrame<C>>) -> io::Result<()> {
        let last_segment = self
            .segments
            .last_mut()
            .unwrap_or_else(|| unreachable!("there should be at least on segment"));
        let mut framed = Framed::new(last_segment, WAL::<C>::new());
        if let Some(DataFrame::Entry(entry)) = item.last() {
            self.next_log_index = entry.index.overflow_add(1);
        }
        framed.send(item).await?;
        framed.flush().await?;
        framed.get_mut().sync_all().await?;

        if framed.get_ref().is_full() {
            self.open_new_segment().await?;
        }

        Ok(())
    }

    /// Tuncate all the logs whose index is less than or equal to `compact_index`
    ///
    /// `compact_index` should be the smallest index required in CURP
    pub(super) async fn truncate_head(&mut self, compact_index: LogIndex) -> io::Result<()> {
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
    pub(super) async fn truncate_tail(&mut self, max_index: LogIndex) -> io::Result<()> {
        // segments to truncate
        let segments = self
            .segments
            .iter_mut()
            .rev()
            .take_while_inclusive::<_>(|s| s.base_index() > max_index);

        for segment in segments {
            segment.seal::<C>(max_index).await;
        }

        let to_remove = self.update_segments();
        SegmentRemover::new_removal(&self.config.dir, to_remove.iter()).await?;

        self.next_log_index = max_index.overflow_add(1);
        self.open_new_segment().await?;

        Ok(())
    }

    /// Opens a new WAL segment
    async fn open_new_segment(&mut self) -> io::Result<()> {
        let lfile = self
            .pipeline
            .next()
            .await
            .ok_or(io::Error::from(io::ErrorKind::BrokenPipe))??;

        let segment = WALSegment::create(
            lfile,
            self.next_log_index,
            self.next_segment_id,
            self.config.max_segment_size,
        )
        .await?;

        self.segments.push(segment);
        self.next_segment_id = self.next_segment_id.overflow_add(1);

        Ok(())
    }

    /// Removes segments that is no long needed
    #[allow(clippy::pattern_type_mismatch)] // Cannot satisfiy both clippy
    fn update_segments(&mut self) -> Vec<WALSegment> {
        let flags: Vec<_> = self.segments.iter().map(WALSegment::is_redundant).collect();
        let (to_remove, remaining): (Vec<_>, Vec<_>) = self
            .segments
            .drain(..)
            .zip(flags.into_iter())
            .partition(|(_, f)| *f);

        self.segments = remaining.into_iter().map(|(s, _)| s).collect();

        to_remove.into_iter().map(|(s, _)| s).collect()
    }

    /// Syncs all flushed segments
    async fn sync_segments(&mut self) -> io::Result<()> {
        let to_sync = self
            .segments
            .iter_mut()
            .filter(|s| matches!(s.io_state(), IOState::Flushed));
        for segment in to_sync {
            segment.sync_all().await?;
        }

        Ok(())
    }

    /// Checks if the log index are continuous
    #[allow(single_use_lifetimes)] // the lifetime is required here
    fn check_log_continuity<'a>(entries: impl Iterator<Item = &'a LogEntry<C>> + Clone) -> bool {
        entries
            .clone()
            .zip(entries.skip(1))
            .all(|(x, y)| x.index.overflow_add(1) == y.index)
    }
}
