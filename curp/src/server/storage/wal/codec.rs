use std::{io, marker::PhantomData};

use bytes::Buf;
use curp_external_api::LogIndex;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio_util::codec::{Decoder, Encoder};

use crate::log_entry::LogEntry;

#[derive(Debug)]
enum WALFrame<C> {
    /// Invalid Frame
    Invalid,
    /// A Frame containing a log entry
    Entry(LogEntry<C>),
    /// A Frame containing the sealed index
    SealIndex(LogIndex),
    /// A Frame containing the checksum
    Commit(Vec<u8>),
}

impl<C> WALFrame<C>
where
    C: Serialize,
{
    fn frame_type(&self) -> u8 {
        match *self {
            WALFrame::Invalid => 0x00,
            WALFrame::Entry(_) => 0x01,
            WALFrame::SealIndex(_) => 0x02,
            WALFrame::Commit(_) => 0x03,
        }
    }

    fn encode_frame(&self) -> Vec<u8> {
        match *self {
            WALFrame::Invalid => unreachable!("trying to encode an invalid frame"),
            WALFrame::Entry(ref entry) => {
                let entry_bytes = bincode::serialize(entry)
                    .unwrap_or_else(|_| unreachable!("serialization should never fail"));
                let len = entry_bytes.len();
                assert_eq!(len >> 56, 0, "log entry length: {len} too large");
                let len_bytes = len.to_le_bytes().take(7);
                let header = [self.frame_type()].into_iter().chain(len_bytes);
                header.chain(entry_bytes).collect()
            }
            WALFrame::SealIndex(index) => {
                assert_eq!(index >> 56, 0, "log index: {index} too large");
                // use the first 7 bytes
                let index_bytes = index.to_le_bytes().into_iter().take(7);
                [self.frame_type()].into_iter().chain(index_bytes).collect()
            }
            WALFrame::Commit(checksum) => {
                let header = [self.frame_type()].chain(&vec![0u8; 7]);

                header.chain(checksum).collect()
            }
        }
    }
}

/// The WAL codec
#[derive(Debug)]
pub(super) struct WAL<C> {
    _phantom: PhantomData<C>,
}

impl<C> Encoder<Vec<WALFrame<C>>> for WAL<C>
where
    C: Serialize,
{
    type Error = io::Error;

    fn encode(
        &mut self,
        entries: Vec<WALFrame<C>>,
        dst: &mut bytes::BytesMut,
    ) -> Result<(), Self::Error> {
        let log_bytes = bincode::serialize(&item)
            .unwrap_or_else(|_| unreachable!("serialization should never fail"));

        let record_len = log_bytes.len() + 32;

        dst.extend(record_len.to_le_bytes());
        dst.extend(Self::get_checksum(&log_bytes));
        dst.extend(log_bytes);

        Ok(())
    }
}

impl<C> Decoder for WAL<C>
where
    C: for<'a> Deserialize<'a>,
{
    type Item = LogEntry<C>;

    type Error = io::Error;

    fn decode(&mut self, src: &mut bytes::BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        // length bytes
        if src.remaining() < 8 {
            return Ok(None);
        }
        let record_len = src.get_u64_le() as usize;
        if src.remaining() < record_len {
            return Ok(None);
        }
        let mut data = src.split_to(record_len);
        let checksum = data.split_to(32);

        if !Self::validate(&data, &checksum) {
            return Err(io::Error::from(io::ErrorKind::InvalidData));
        }

        let log_entry = bincode::deserialize(&data)
            .unwrap_or_else(|_| unreachable!("deserialization should never fail"));

        Ok(log_entry)
    }
}

impl<C> WAL<C> {
    pub(super) fn new() -> Self {
        Self {
            _phantom: PhantomData,
        }
    }

    fn get_checksum(data: &[u8]) -> Vec<u8> {
        let mut hasher = Sha256::new();
        hasher.update(&data);
        hasher.finalize().into_iter().collect()
    }

    fn validate(data: &[u8], checksum: &[u8]) -> bool {
        &Self::get_checksum(data)[..] == checksum
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
