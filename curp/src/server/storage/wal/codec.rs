use std::{io, marker::PhantomData};

use curp_external_api::LogIndex;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio_util::codec::{Decoder, Encoder};

use crate::log_entry::LogEntry;

trait FrameType {
    fn frame_type(&self) -> u8;
}

trait FrameEncoder {
    fn encode(&self) -> Vec<u8>;
}

#[derive(Debug)]
enum WALFrame<C> {
    Data(DataFrame<C>),
    Commit(CommitFrame),
}

impl<C> WALFrame<C>
where
    C: for<'a> Deserialize<'a>,
{
    #[allow(clippy::indexing_slicing)] // The indexing is safe
    fn decode(src: &[u8]) -> io::Result<Option<(Self, usize)>> {
        if src.len() < 8 {
            return Ok(None);
        }
        let header: [u8; 8] = src[0..8]
            .try_into()
            .unwrap_or_else(|_| unreachable!("this conversion will always succeed"));
        let frame_type = header[0];
        Ok(Some(match frame_type {
            0x01 => {
                let len = Self::get_u64(header) as usize;
                if src.len() < 8 + len {
                    return Ok(None);
                }
                let payload = &src[8..8 + len];
                let entry: LogEntry<C> = bincode::deserialize(payload)
                    .map_err(|_| io::Error::from(io::ErrorKind::InvalidData))?;
                (Self::Data(DataFrame::Entry(entry)), 8 + len)
            }
            0x02 => {
                let index = Self::get_u64(header);
                (Self::Data(DataFrame::SealIndex(index)), 8)
            }
            0x03 => {
                if src.len() < 8 + 32 {
                    return Ok(None);
                }
                let checksum = src[8..8 + 32].to_vec();
                (Self::Commit(CommitFrame { checksum }), 8 + 32)
            }
            _ => {
                return Err(io::Error::from(io::ErrorKind::Unsupported));
            }
        }))
    }

    fn get_u64(mut header: [u8; 8]) -> u64 {
        header.rotate_left(1);
        header[7] = 0;
        u64::from_le_bytes(header)
    }
}

#[derive(Debug)]
pub(crate) enum DataFrame<C> {
    /// A Frame containing a log entry
    Entry(LogEntry<C>),
    /// A Frame containing the sealed index
    SealIndex(LogIndex),
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
    fn encode(&self) -> Vec<u8> {
        match *self {
            DataFrame::Entry(ref entry) => {
                let entry_bytes = bincode::serialize(entry)
                    .unwrap_or_else(|_| unreachable!("serialization should never fail"));
                let len = entry_bytes.len();
                assert_eq!(len >> 56, 0, "log entry length: {len} too large");
                let len_bytes = len.to_le_bytes().into_iter().take(7);
                let header = [self.frame_type()].into_iter().chain(len_bytes);
                header.chain(entry_bytes).collect()
            }
            DataFrame::SealIndex(index) => {
                assert_eq!(index >> 56, 0, "log index: {index} too large");
                // use the first 7 bytes
                let index_bytes = index.to_le_bytes().into_iter().take(7);
                [self.frame_type()].into_iter().chain(index_bytes).collect()
            }
        }
    }
}

#[derive(Debug)]
struct CommitFrame {
    checksum: Vec<u8>,
}

impl CommitFrame {
    fn new_from_data(data: &[u8]) -> Self {
        Self {
            checksum: Self::get_checksum(data),
        }
    }

    /// Get the checksum of the slice, we use Sha256 as the hash function
    fn get_checksum(data: &[u8]) -> Vec<u8> {
        let mut hasher = Sha256::new();
        hasher.update(data);
        hasher.finalize().into_iter().collect()
    }

    fn validate(&self, data: &[u8]) -> bool {
        self.checksum == Self::get_checksum(data)
    }
}

impl FrameType for CommitFrame {
    fn frame_type(&self) -> u8 {
        0x03
    }
}

impl FrameEncoder for CommitFrame {
    fn encode(&self) -> Vec<u8> {
        let header = [self.frame_type()].into_iter().chain([0u8; 7].into_iter());
        header.chain(self.checksum.to_owned()).collect()
    }
}

/// The WAL codec
#[derive(Debug)]
pub(crate) struct WAL<C> {
    _phantom: PhantomData<C>,
}

impl<C, D> Encoder<D> for WAL<C>
where
    C: Serialize,
    D: Iterator<Item = DataFrame<C>>,
{
    type Error = io::Error;

    fn encode(&mut self, frames: D, dst: &mut bytes::BytesMut) -> Result<(), Self::Error> {
        let frames_bytes: Vec<_> = frames.into_iter().map(|f| f.encode()).flatten().collect();
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

    type Error = io::Error;

    fn decode(&mut self, src: &mut bytes::BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        let mut frames = vec![];
        loop {
            match WALFrame::<C>::decode(src)? {
                Some((frame, len)) => {
                    let _ignore = src.split_to(len);
                    match frame {
                        WALFrame::Data(data) => {
                            frames.push(data);
                        }
                        WALFrame::Commit(commit) => {
                            let frames_bytes: Vec<_> =
                                frames.iter().map(|d| d.encode()).flatten().collect();
                            if commit.validate(&frames_bytes) {
                                return Ok(Some(frames));
                            } else {
                                return Err(io::Error::from(io::ErrorKind::InvalidData));
                            }
                        }
                    }
                }
                None => return Ok(None),
            }
        }
    }
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
    #[allow(unused)]
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
