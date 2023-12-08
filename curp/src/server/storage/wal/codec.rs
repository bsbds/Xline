use std::{io, marker::PhantomData};

use clippy_utilities::NumericCast;
use curp_external_api::LogIndex;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;
use tokio_util::codec::{Decoder, Encoder};

use crate::log_entry::LogEntry;

use super::{
    error::{CorruptError, WALError},
    util::{get_checksum, validate_data},
};

/// Getting the frame type
trait FrameType {
    /// Returns the type of this frame
    fn frame_type(&self) -> u8;
}

/// Encoding of frames
trait FrameEncoder {
    /// Encodes the current frame
    fn encode(&self) -> Vec<u8>;
}

/// The WAL codec
#[allow(clippy::upper_case_acronyms)] // The WAL needs to be all upper cases
#[derive(Debug)]
pub(super) struct WAL<C> {
    /// Frames stored in decoding
    frames: Vec<DataFrame<C>>,
    /// The hasher state for decoding
    hasher: Sha256,
}

/// Union type of WAL frames
#[derive(Debug)]
enum WALFrame<C> {
    /// Data frame type
    Data(DataFrame<C>),
    /// Commit frame type
    Commit(CommitFrame),
}

/// The data frame
///
/// Contains either a log entry or a seal index
#[derive(Debug, Clone)]
#[cfg_attr(test, derive(PartialEq))]
pub(super) enum DataFrame<C> {
    /// A Frame containing a log entry
    Entry(LogEntry<C>),
    /// A Frame containing the sealed index
    SealIndex(LogIndex),
}

/// The commit frame
///
/// This frames contains a SHA256 checksum of all previous frames since last commit
#[derive(Debug)]
struct CommitFrame {
    /// The SHA256 checksum
    checksum: Vec<u8>,
}

impl<C> WAL<C> {
    /// Creates a new WAL codec
    pub(super) fn new() -> Self {
        Self {
            frames: Vec::new(),
            hasher: Sha256::new(),
        }
    }

    /// Gets encoded length and padding length
    ///
    /// This is used to prevent torn write by forcing 8-bit alignment
    #[allow(unused, clippy::integer_arithmetic)] // TODO: 8bit alignment
    fn encode_frame_size(data_len: usize) -> (usize, usize) {
        let mut encoded_len = data_len;
        let pad_len = (8 - data_len % 8) % 8;
        if pad_len != 0 {
            // encode padding info
            encoded_len |= (0x80 | pad_len) << 56;
        }
        (encoded_len, pad_len)
    }
}

impl<C> Encoder<Vec<DataFrame<C>>> for WAL<C>
where
    C: Serialize,
{
    type Error = io::Error;

    fn encode(
        &mut self,
        frames: Vec<DataFrame<C>>,
        dst: &mut bytes::BytesMut,
    ) -> Result<(), Self::Error> {
        let frames_bytes: Vec<_> = frames.into_iter().flat_map(|f| f.encode()).collect();
        let commit_frame = CommitFrame::new_from_data(&frames_bytes);

        dst.extend(frames_bytes);
        dst.extend(commit_frame.encode());

        Ok(())
    }
}

impl<C> Decoder for WAL<C>
where
    C: Serialize + for<'a> Deserialize<'a>,
{
    type Item = Vec<DataFrame<C>>;

    type Error = WALError;

    fn decode(&mut self, src: &mut bytes::BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        loop {
            if let Some((frame, len)) = WALFrame::<C>::decode(src)? {
                let decoded_bytes = src.split_to(len);
                match frame {
                    WALFrame::Data(data) => {
                        self.frames.push(data);
                        self.hasher.update(decoded_bytes);
                    }
                    WALFrame::Commit(commit) => {
                        let frames_bytes: Vec<_> =
                            self.frames.iter().flat_map(DataFrame::encode).collect();
                        let checksum = self.hasher.clone().finalize();
                        self.hasher.reset();
                        if commit.validate(&checksum) {
                            return Ok(Some(self.frames.drain(..).collect()));
                        }
                        return Err(WALError::Corrupted(CorruptError::Checksum));
                    }
                }
            } else {
                return Ok(None);
            }
        }
    }
}

#[allow(
    clippy::indexing_slicing, // Index slicings are checked
    clippy::integer_arithmetic, //  Arithmetics are checked
    clippy::unnecessary_wraps // Use the wraps to make code more consistenct
)]
impl<C> WALFrame<C>
where
    C: for<'a> Deserialize<'a>,
{
    /// Decodes a frame from the buffer
    ///
    /// * The frame header memory layout
    ///
    /// 0      1      2      3      4      5      6      7      8
    /// |------+------+------+------+------+------+------+------|
    /// | Type | Length / Index / Reserved                      |
    /// |------+------+------+------+------+------+------+------|
    ///
    /// * The frame types
    ///
    /// |------------+-------+-------------------------------------------------------|
    /// | Type       | Value | Desc                                                  |
    /// |------------+-------+-------------------------------------------------------|
    /// | Invalid    |  0x00 | Invalid type                                          |
    /// | Entry      |  0x01 | Stores record of CURP log entry                       |
    /// | Seal Index |  0x02 | Stores the highest index of this current sealed frame |
    /// | Commit     |  0x03 | Stores the checksum                                   |
    /// |------------+-------+-------------------------------------------------------|
    fn decode(src: &[u8]) -> Result<Option<(Self, usize)>, WALError> {
        if src.len() < 8 {
            return Ok(None);
        }
        let header: [u8; 8] = src[0..8]
            .try_into()
            .unwrap_or_else(|_| unreachable!("this conversion will always succeed"));
        let frame_type = header[0];
        match frame_type {
            0x00 => Err(WALError::MaybeEnded),
            0x01 => Self::decode_entry(header, &src[8..]),
            0x02 => Self::decode_seal_index(header),
            0x03 => Self::decode_commit(header, &src[8..]),
            _ => Err(WALError::Corrupted(CorruptError::Codec(
                "Unexpected frame type".to_owned(),
            ))),
        }
    }

    /// Decodes an entry frame from source
    fn decode_entry(header: [u8; 8], src: &[u8]) -> Result<Option<(Self, usize)>, WALError> {
        let len: usize = Self::get_u64(header).numeric_cast();
        if src.len() < len {
            return Ok(None);
        }
        let payload = &src[..len];
        let entry: LogEntry<C> = bincode::deserialize(payload)
            .map_err(|e| WALError::Corrupted(CorruptError::Codec(e.to_string())))?;

        Ok(Some((Self::Data(DataFrame::Entry(entry)), 8 + len)))
    }

    /// Decodes an seal index frame from source
    fn decode_seal_index(header: [u8; 8]) -> Result<Option<(Self, usize)>, WALError> {
        let index = Self::get_u64(header);

        Ok(Some((Self::Data(DataFrame::SealIndex(index)), 8)))
    }

    /// Decodes an commit frame from source
    fn decode_commit(header: [u8; 8], src: &[u8]) -> Result<Option<(Self, usize)>, WALError> {
        if src.len() < 32 {
            return Ok(None);
        }
        if !header.iter().skip(1).all(|b| *b == 0) {
            return Err(WALError::Corrupted(CorruptError::Codec(
                "Bitflip detected in commit frame header".to_owned(),
            )));
        }

        let checksum = src[..32].to_vec();

        Ok(Some((Self::Commit(CommitFrame { checksum }), 8 + 32)))
    }

    /// Gets log index or length encoded using 7 bytes
    fn get_u64(mut header: [u8; 8]) -> u64 {
        header.rotate_left(1);
        header[7] = 0;
        u64::from_le_bytes(header)
    }
}

impl<C> FrameType for DataFrame<C> {
    fn frame_type(&self) -> u8 {
        match *self {
            DataFrame::Entry(_) => 0x01,
            DataFrame::SealIndex(_) => 0x02,
        }
    }
}

impl<C> FrameEncoder for DataFrame<C>
where
    C: Serialize,
{
    #[allow(clippy::integer_arithmetic)] // The integer shift is safe
    fn encode(&self) -> Vec<u8> {
        match *self {
            DataFrame::Entry(ref entry) => {
                let entry_bytes = bincode::serialize(entry)
                    .unwrap_or_else(|_| unreachable!("serialization should never fail"));
                let len = entry_bytes.len();
                assert_eq!(len >> 56, 0, "log entry length: {len} too large");
                let len_bytes = len.to_le_bytes().into_iter().take(7);
                let header = std::iter::once(self.frame_type()).chain(len_bytes);
                header.chain(entry_bytes).collect()
            }
            DataFrame::SealIndex(index) => {
                assert_eq!(index >> 56, 0, "log index: {index} too large");
                // use the first 7 bytes
                let index_bytes = index.to_le_bytes().into_iter().take(7);
                std::iter::once(self.frame_type())
                    .chain(index_bytes)
                    .collect()
            }
        }
    }
}

impl CommitFrame {
    /// Creates a commit frame of data
    fn new_from_data(data: &[u8]) -> Self {
        Self {
            checksum: get_checksum(data),
        }
    }

    /// Validates the checksum
    fn validate(&self, checksum: &[u8]) -> bool {
        *checksum == self.checksum
    }
}

impl FrameType for CommitFrame {
    fn frame_type(&self) -> u8 {
        0x03
    }
}

impl FrameEncoder for CommitFrame {
    fn encode(&self) -> Vec<u8> {
        let header = std::iter::once(self.frame_type()).chain([0u8; 7].into_iter());
        header.chain(self.checksum.clone()).collect()
    }
}

#[cfg(test)]
mod tests {
    use curp_external_api::cmd::ProposeId;
    use curp_test_utils::test_cmd::TestCommand;
    use futures::{future::join_all, FutureExt, SinkExt};
    use tempfile::tempfile;
    use tokio::{
        fs::File as TokioFile,
        io::{AsyncSeekExt, AsyncWrite, AsyncWriteExt, DuplexStream},
    };
    use tokio_stream::StreamExt;
    use tokio_util::codec::Framed;

    use crate::{log_entry::EntryData, server::storage::wal::test_util::EntryGenerator};

    use super::*;

    #[tokio::test]
    async fn frame_encode_decode_is_ok() {
        let mut buffer = IOBuffer::new_empty();
        let mut framed = Framed::new(buffer.new_cursor(), WAL::<TestCommand>::new());
        let mut entry_gen = EntryGenerator::new(0);
        let num_entry_frames = 100;
        let num_seal_frames = 100;

        let entry_frames = send_entry_frames(&mut framed, &mut entry_gen, num_entry_frames).await;
        let seal_frames = send_seal_frames(&mut framed, num_entry_frames).await;

        let mut framed = Framed::new(buffer.new_cursor(), WAL::<TestCommand>::new());

        let frames: Vec<_> = framed
            .collect::<Result<Vec<_>, _>>()
            .await
            .unwrap()
            .into_iter()
            .flatten()
            .collect();

        let (entry_frames_decoded, seal_frames_decoded) = frames.split_at(num_entry_frames);
        assert_eq!(entry_frames, entry_frames_decoded);
        assert_eq!(seal_frames, seal_frames_decoded);
    }

    #[tokio::test]
    async fn wal_will_end_at_read_zero() {
        let mut buffer = IOBuffer::alloc(1024);
        let mut framed = Framed::new(buffer.new_cursor(), WAL::<TestCommand>::new());
        let mut entry_gen = EntryGenerator::new(0);

        let _e = send_entry_frames(&mut framed, &mut entry_gen, 1).await;

        let mut framed = Framed::new(buffer.new_cursor(), WAL::<TestCommand>::new());

        assert!(framed.next().await.unwrap().is_ok());
        let err = framed.next().await.unwrap().unwrap_err();
        assert!(matches!(err, WALError::MaybeEnded), "error {err} not match");
    }

    #[tokio::test]
    async fn frame_zero_write_will_be_detected() {
        let mut buffer = IOBuffer::new_empty();
        let mut framed = Framed::new(buffer.new_cursor(), WAL::<TestCommand>::new());
        let mut entry_gen = EntryGenerator::new(0);

        let _e = send_entry_frames(&mut framed, &mut entry_gen, 1).await;

        /// zero the first byte, it will reach a success state,
        /// all following data will be truncated
        let mut framed = Framed::new(
            buffer.new_cursor_with(IOBuffer::zero_offset(0, 1)),
            WAL::<TestCommand>::new(),
        );

        let err = framed.next().await.unwrap().unwrap_err();
        assert!(matches!(err, WALError::MaybeEnded), "error {err} not match");
    }

    #[tokio::test]
    async fn single_byte_corruption_will_be_detected() {
        let mut buffer = IOBuffer::new_empty();
        let mut framed = Framed::new(buffer.new_cursor(), WAL::<TestCommand>::new());
        let mut entry_gen = EntryGenerator::new(0);
        let num_entry_frames = 10;
        let num_seal_frames = 10;

        let _e = send_entry_frames(&mut framed, &mut entry_gen, num_entry_frames).await;
        let _s = send_seal_frames(&mut framed, num_entry_frames).await;

        for i in 0..buffer.len() {
            let mut framed = Framed::new(
                buffer.new_cursor_with(IOBuffer::corrupt_offset(i, 1)),
                WAL::<TestCommand>::new(),
            );

            assert!(framed
                .take(num_entry_frames + num_seal_frames)
                .collect::<Result<Vec<_>, _>>()
                .await
                .is_err());
        }
    }

    #[tokio::test]
    async fn spray_corruption_will_be_detected() {
        let mut buffer = IOBuffer::new_empty();
        let mut framed = Framed::new(buffer.new_cursor(), WAL::<TestCommand>::new());
        let mut entry_gen = EntryGenerator::new(0);
        let num_entry_frames = 100;
        let num_seal_frames = 100;

        let _e = send_entry_frames(&mut framed, &mut entry_gen, num_entry_frames).await;
        let _s = send_seal_frames(&mut framed, num_entry_frames).await;

        // Range from 0.01% to 100% with step 0.01%
        for percentage in (1..=100 * 100).map(|x| x as f64 * 0.01) {
            let mut framed = Framed::new(
                buffer.new_cursor_with(IOBuffer::corrupt_spray(percentage)),
                WAL::<TestCommand>::new(),
            );
            assert!(framed
                .take(num_entry_frames + num_seal_frames)
                .collect::<Result<Vec<_>, _>>()
                .await
                .is_err());
        }
    }

    #[tokio::test]
    async fn unsynced_data_will_be_truncate() {
        let mut buffer = IOBuffer::new_empty();
        let mut framed = Framed::new(buffer.new_cursor(), WAL::<TestCommand>::new());
        let mut entry_gen = EntryGenerator::new(0);
        let num_entry_frames = 10;
        let num_seal_frames = 10;

        let mut all_frames = vec![];
        all_frames.extend(send_entry_frames(&mut framed, &mut entry_gen, num_entry_frames).await);
        all_frames.extend(send_seal_frames(&mut framed, num_entry_frames).await);

        let mut frame_nums = vec![];
        // simulate drop of unsynced data
        let len = buffer.len();
        for unsynced_data_len in 1..=len {
            let mut framed = Framed::new(
                buffer.new_cursor_with(IOBuffer::zero_offset(
                    len - unsynced_data_len,
                    unsynced_data_len,
                )),
                WAL::<TestCommand>::new(),
            );

            let frames: Vec<_> = framed
                .take_while(|res| res.is_ok())
                .collect::<Result<Vec<_>, _>>()
                .await
                .unwrap()
                .into_iter()
                .flatten()
                .collect();

            assert!(all_frames.iter().zip(frames.iter()).all(|(x, y)| x == y));
            frame_nums.push(frames.len());
        }

        // frames num recoverd is non ascending
        assert!(frame_nums
            .iter()
            .zip(frame_nums.iter().skip(1))
            .all(|(x, y)| *x >= *y));
    }

    async fn send_entry_frames<T: AsyncWrite + Unpin>(
        framed: &mut Framed<T, WAL<TestCommand>>,
        entry_gen: &mut EntryGenerator,
        num: usize,
    ) -> Vec<DataFrame<TestCommand>> {
        let entry_frames: Vec<_> = entry_gen
            .take(num)
            .into_iter()
            .map(|e| DataFrame::Entry(e))
            .collect();
        framed.send(entry_frames.clone()).await.unwrap();
        framed.get_mut().flush().await;
        entry_frames
    }

    async fn send_seal_frames<T: AsyncWrite + Unpin>(
        framed: &mut Framed<T, WAL<TestCommand>>,
        num: usize,
    ) -> Vec<DataFrame<TestCommand>> {
        let seal_frames: Vec<_> = (1..=num)
            .map(|i| DataFrame::<TestCommand>::SealIndex(i as u64))
            .collect();
        framed.send(seal_frames.clone()).await.unwrap();
        framed.get_mut().flush().await;
        seal_frames
    }

    type CorruptFn = Box<dyn Fn(Vec<u8>) -> Vec<u8>>;

    struct IOBuffer {
        inner: Vec<u8>,
    }

    impl IOBuffer {
        fn new_empty() -> Self {
            Self { inner: vec![] }
        }

        fn alloc(size: usize) -> Self {
            Self {
                inner: vec![0; size],
            }
        }

        fn len(&self) -> usize {
            self.inner.len()
        }

        fn new_cursor(&mut self) -> io::Cursor<&mut Vec<u8>> {
            io::Cursor::new(&mut self.inner)
        }

        fn new_cursor_with(&self, corrupt_fn: CorruptFn) -> io::Cursor<Vec<u8>> {
            io::Cursor::new(corrupt_fn(self.inner.clone()))
        }

        fn zero_offset(offset: usize, len: usize) -> CorruptFn {
            Box::new(move |mut buf: Vec<u8>| {
                buf[offset..offset + len].fill(0);
                buf
            })
        }

        fn corrupt_offset(offset: usize, len: usize) -> CorruptFn {
            Box::new(move |mut buf: Vec<u8>| {
                for byte in &mut buf[offset..offset + len] {
                    *byte = !*byte;
                }
                buf
            })
        }

        fn zero_spray(percentage: f64) -> CorruptFn {
            Box::new(move |mut buf: Vec<u8>| {
                let total = buf.len();
                let zero_num = (total as f64 * percentage) as usize;
                for chunk in buf.chunks_mut(zero_num) {
                    chunk.fill(0);
                }
                buf
            })
        }

        fn corrupt_spray(percentage: f64) -> CorruptFn {
            Box::new(move |mut buf: Vec<u8>| {
                let total = buf.len();
                let corrupt_num = (total as f64 * percentage) as usize;
                for (i, chunk) in buf.chunks_mut(corrupt_num).enumerate() {
                    // make it more evenly distributed while keepping test deteminisim
                    let pos = i % chunk.len();
                    let flip_pos = i % 8;
                    chunk[pos] = chunk[pos] ^ (1 << flip_pos);
                }
                buf
            })
        }
    }
}
