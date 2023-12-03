use std::{io, iter, pin::Pin, sync::Arc, task::Poll};

use clippy_utilities::{NumericCast, OverflowArithmetic};
use curp_external_api::LogIndex;
use futures::{ready, FutureExt, SinkExt};
use serde::Serialize;
use tokio::{
    fs::File as TokioFile,
    io::{AsyncRead, AsyncReadExt, AsyncSeekExt, AsyncWrite, AsyncWriteExt},
    sync::Mutex,
};
use tokio_util::codec::Framed;

use crate::server::storage::wal::WAL_FILE_EXT;

use super::{
    codec::{DataFrame, WAL},
    util::{get_checksum, validate_data, LockedFile},
    WAL_MAGIC, WAL_VERSION,
};

/// The size of wal file header in bytes
const WAL_HEADER_SIZE: usize = 56;

#[derive(Debug)]
pub(super) struct WALSegment {
    /// The inner state
    inner: Inner,
    /// The base index of this segment
    base_index: LogIndex,
    /// The id of this segment
    segment_id: u64,
}

#[derive(Debug)]
struct Inner {
    /// The opened file of this segment
    file: TokioFile,
    /// The file size of the segment
    size: u64,
    /// The highest index of the segment
    seal_index: LogIndex,
    /// The IO state of the file
    io_state: IOState,
}

/// The IO state of the file
#[derive(Clone, Copy, Debug, Default)]
pub(super) enum IOState {
    /// The initial state that haven't written any data or fsynced
    #[default]
    Fsynced,
    /// Already wrote some data, but haven't flushed yet
    Written,
    /// Already flushed, but haven't called fsync yet
    Flushed,
    /// Shutdowned
    Shutdowned,
    /// The IO has failed on this file
    Errored,
}

impl WALSegment {
    /// Open an existing WAL segment file
    pub(super) async fn open(lfile: LockedFile) -> io::Result<Self> {
        let mut file = lfile.into_async();
        let size = file.metadata().await?.len();
        let mut buf = vec![0; WAL_HEADER_SIZE];
        let _ignore = file.read_exact(&mut buf).await?;
        let (base_index, segment_id) = Self::parse_header(&buf)?;

        let inner = Inner {
            file,
            size,
            // Index 0 means the seal_index hasn't been read yet
            seal_index: 0,
            io_state: IOState::default(),
        };

        Ok(Self {
            inner,
            base_index,
            segment_id,
        })
    }

    /// Creates a new `WALSegment`
    pub(super) async fn create(
        tmp_file: LockedFile,
        base_index: LogIndex,
        segment_id: u64,
    ) -> io::Result<Self> {
        let segment_name = Self::segment_name(segment_id, base_index);
        let lfile = tmp_file.rename(segment_name)?;
        let mut file = lfile.into_async();
        file.write_all(&Self::gen_header(base_index, segment_id))
            .await?;
        file.flush().await?;
        file.sync_all().await?;

        let inner = Inner {
            file,
            size: WAL_HEADER_SIZE.numeric_cast(),
            // For convenience we set it to largest u64 value that represent not sealed
            seal_index: u64::MAX,
            io_state: IOState::default(),
        };

        Ok(Self {
            inner,
            base_index,
            segment_id,
        })
    }

    /// Seal the current segment
    ///
    /// After the seal, the log index in this segment should be less than `next_index`
    pub(super) async fn seal<C: Serialize>(&mut self, next_index: LogIndex) {
        let mut framed = Framed::new(self, WAL::<C>::new());
        framed.send(vec![DataFrame::SealIndex(next_index)]).await;
        framed.flush().await;
        framed.get_mut().sync_all().await;
        framed.get_mut().update_seal_index(next_index);
    }

    pub(super) fn size(&self) -> u64 {
        self.inner.size
    }

    pub(super) fn update_seal_index(&mut self, index: LogIndex) {
        self.inner.seal_index = self.inner.seal_index.max(index);
    }

    pub(super) async fn sync_all(&self) -> io::Result<()> {
        self.inner.file.sync_all().await
    }

    pub(super) async fn reset_offset(&mut self) -> io::Result<()> {
        self.inner
            .file
            .seek(io::SeekFrom::Start(0))
            .await
            .map(|_| ())
    }

    pub(super) fn id(&self) -> u64 {
        self.segment_id
    }

    pub(super) fn base_index(&self) -> u64 {
        self.base_index
    }

    pub(super) fn is_redundant(&self) -> bool {
        self.inner.seal_index < self.base_index
    }

    pub(super) fn state(&self) -> IOState {
        self.inner.io_state
    }

    /// Gets the file name of the WAL segment
    pub(super) fn segment_name(segment_id: u64, log_index: u64) -> String {
        format!("{:016x}-{:016x}{WAL_FILE_EXT}", segment_id, log_index)
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
        buf.extend(vec![0; 3]);
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
            || next_field(3) != &[0; 3]
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

impl AsyncWrite for WALSegment {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, io::Error>> {
        match ready!(Pin::new(&mut self.inner.file).poll_write(cx, buf)) {
            Ok(len) => {
                self.inner.io_state.written();
                self.inner.size = self.inner.size.overflow_add(len.numeric_cast());
                Poll::Ready(Ok(len))
            }
            Err(e) => {
                self.inner.io_state.errored();
                Poll::Ready(Err(e))
            }
        }
    }

    fn poll_flush(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Result<(), io::Error>> {
        match ready!(Pin::new(&mut self.inner.file).poll_flush(cx)) {
            Ok(()) => {
                self.inner.io_state.flushed();
                Poll::Ready(Ok(()))
            }
            Err(e) => {
                self.inner.io_state.errored();
                Poll::Ready(Err(e))
            }
        }
    }

    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Result<(), io::Error>> {
        match ready!(Pin::new(&mut self.inner.file).poll_shutdown(cx)) {
            Ok(()) => {
                self.inner.io_state.shutdowned();
                Poll::Ready(Ok(()))
            }
            Err(e) => {
                self.inner.io_state.errored();
                Poll::Ready(Err(e))
            }
        }
    }
}

impl AsyncRead for WALSegment {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner.file).poll_read(cx, buf)
    }
}

impl PartialEq for WALSegment {
    fn eq(&self, other: &Self) -> bool {
        self.segment_id.eq(&other.segment_id)
    }
}

impl Eq for WALSegment {}

impl PartialOrd for WALSegment {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.segment_id.partial_cmp(&other.segment_id)
    }
}

impl Ord for WALSegment {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.segment_id.cmp(&other.segment_id)
    }
}

impl IOState {
    /// Mutate the state to `IOState::Written`
    ///
    /// # Panics
    ///
    /// This method panics if the state is not `IOState::Fsynced`
    fn written(&mut self) {
        assert!(matches!(self, IOState::Fsynced));
        *self = IOState::Written
    }

    /// Mutate the state to `IOState::Flushed`
    ///
    /// # Panics
    ///
    /// This method panics if the state is not `IOState::Written`
    fn flushed(&mut self) {
        assert!(matches!(self, IOState::Written));
        *self = IOState::Flushed
    }

    /// Mutate the state to `IOState::Written`
    ///
    /// # Panics
    ///
    /// This method panics if the state is not `IOState::Flushed`
    fn fsynced(&mut self) {
        assert!(matches!(self, IOState::Flushed));
        *self = IOState::Fsynced
    }

    /// Mutate the state to `IOState::Errored`
    fn errored(&mut self) {
        *self = IOState::Errored
    }

    /// Mutate the state to `IOState::Shutdowned`
    fn shutdowned(&mut self) {
        *self = IOState::Shutdowned
    }
}
