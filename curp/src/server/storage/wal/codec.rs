use std::{io, marker::PhantomData};

use clippy_utilities::NumericCast;
use curp_external_api::LogIndex;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;
use tokio_util::codec::{Decoder, Encoder};

use crate::log_entry::LogEntry;

use super::{
    error::{CorruptType, WALError},
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
#[derive(Debug)]
pub(crate) struct WAL<C> {
    /// The phantom data
    _phantom: PhantomData<C>,
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
pub(crate) enum DataFrame<C> {
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
            _phantom: PhantomData,
        }
    }

    /// Get encoded length and padding length
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
        let mut frames = vec![];
        loop {
            if let Some((frame, len)) = WALFrame::<C>::decode(src)? {
                let _ignore = src.split_to(len);
                match frame {
                    WALFrame::Data(data) => {
                        frames.push(data);
                    }
                    WALFrame::Commit(commit) => {
                        let frames_bytes: Vec<_> =
                            frames.iter().flat_map(DataFrame::encode).collect();
                        if commit.validate(&frames_bytes) {
                            return Ok(Some(frames));
                        }
                        return Err(WALError::Corrupted(CorruptType::Checksum));
                    }
                }
            } else {
                return Ok(None);
            }
        }
    }
}

#[allow(clippy::indexing_slicing, clippy::integer_arithmetic)] // The indexing is safe, and
                                                               // arithmetic are checked
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
            0x00 => return Err(WALError::MaybeEnded),
            0x01 => Self::decode_entry(header, &src[8..]),
            0x02 => Self::decode_seal_index(header),
            0x03 => Self::decode_commit(&src[8..]),
            _ => {
                return Err(WALError::Corrupted(CorruptType::Codec(
                    "Unexpected frame type".to_string(),
                )));
            }
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
            .map_err(|e| WALError::Corrupted(CorruptType::Codec(e.to_string())))?;

        Ok(Some((Self::Data(DataFrame::Entry(entry)), 8 + len)))
    }

    /// Decodes an seal index frame from source
    fn decode_seal_index(header: [u8; 8]) -> Result<Option<(Self, usize)>, WALError> {
        let index = Self::get_u64(header);

        Ok(Some((Self::Data(DataFrame::SealIndex(index)), 8)))
    }

    /// Decodes an commmit frame from source
    fn decode_commit(src: &[u8]) -> Result<Option<(Self, usize)>, WALError> {
        if src.len() < 32 {
            return Ok(None);
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

    /// Validate the checksum
    fn validate(&self, data: &[u8]) -> bool {
        validate_data(data, &self.checksum)
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
    use futures::SinkExt;
    use tempfile::tempfile;
    use tokio::{
        fs::File as TokioFile,
        io::{AsyncSeekExt, AsyncWriteExt, DuplexStream},
    };
    use tokio_stream::StreamExt;
    use tokio_util::codec::Framed;

    use crate::log_entry::EntryData;

    use super::*;

    #[tokio::test]
    async fn frame_encode_decode_is_ok() {
        let file = TokioFile::from(tempfile().unwrap());
        let mut framed = Framed::new(file, WAL::<TestCommand>::new());
        let entry = LogEntry::<TestCommand>::new(1, 1, EntryData::Empty(ProposeId(1, 2)));
        let data_frame = DataFrame::Entry(entry.clone());
        let seal_frame = DataFrame::<TestCommand>::SealIndex(1);
        framed.send(vec![data_frame]).await.unwrap();
        framed.send(vec![seal_frame]).await.unwrap();
        framed.get_mut().flush().await;

        let mut file = framed.into_inner();
        file.seek(io::SeekFrom::Start(0)).await.unwrap();
        let mut framed = Framed::new(file, WAL::<TestCommand>::new());

        let data_frame_get = &framed.next().await.unwrap().unwrap()[0];
        let seal_frame_get = &framed.next().await.unwrap().unwrap()[0];
        let DataFrame::Entry(entry_get) = data_frame_get else {
            panic!("frame should be type: DataFrame::Entry");
        };
        let DataFrame::SealIndex(index) = seal_frame_get else {
            panic!("frame should be type: DataFrame::Entry");
        };

        assert_eq!(*entry_get, entry);
        assert_eq!(*index, 1);
    }

    #[tokio::test]
    async fn frame_zero_write_will_be_detected() {
        let file = TokioFile::from(tempfile().unwrap());
        let mut framed = Framed::new(file, WAL::<TestCommand>::new());
        let entry = LogEntry::<TestCommand>::new(1, 1, EntryData::Empty(ProposeId(1, 2)));
        let data_frame = DataFrame::Entry(entry.clone());
        framed.send(vec![data_frame]).await.unwrap();
        framed.get_mut().flush().await;

        let mut file = framed.into_inner();
        /// zero the first byte, it will reach a success state,
        /// all following data will be truncated
        file.seek(io::SeekFrom::Start(0)).await.unwrap();
        file.write_u8(0).await;

        file.seek(io::SeekFrom::Start(0)).await.unwrap();

        let mut framed = Framed::new(file, WAL::<TestCommand>::new());

        let err = framed.next().await.unwrap().unwrap_err();
        assert!(matches!(err, WALError::MaybeEnded), "error {err} not match");
    }

    #[tokio::test]
    async fn frame_corrupt_will_be_detected() {
        let file = TokioFile::from(tempfile().unwrap());
        let mut framed = Framed::new(file, WAL::<TestCommand>::new());
        let entry = LogEntry::<TestCommand>::new(1, 1, EntryData::Empty(ProposeId(1, 2)));
        let data_frame = DataFrame::Entry(entry.clone());
        framed.send(vec![data_frame]).await.unwrap();
        framed.get_mut().flush().await;

        let mut file = framed.into_inner();
        /// This will cause a failure state
        file.seek(io::SeekFrom::Start(1)).await.unwrap();
        file.write_u8(0).await;

        file.seek(io::SeekFrom::Start(0)).await.unwrap();

        let mut framed = Framed::new(file, WAL::<TestCommand>::new());

        let err = framed.next().await.unwrap().unwrap_err();
        assert!(
            matches!(err, WALError::Corrupted(_)),
            "error {err} not match"
        );
    }
}
