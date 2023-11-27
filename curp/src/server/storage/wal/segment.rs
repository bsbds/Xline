use std::io;

use curp_external_api::LogIndex;
use tokio::{
    fs::File as TokioFile,
    io::{AsyncReadExt, AsyncWriteExt},
};

use super::{
    util::{get_checksum, validate_data, LockedFile},
    WAL_MAGIC, WAL_VERSION,
};

#[derive(Debug)]
pub(super) struct WALSegment {
    file: TokioFile,
    base_index: LogIndex,
    segment_id: u64,
}

impl WALSegment {
    /// Open an existing WAL segment file
    pub(super) async fn open(lfile: LockedFile) -> io::Result<Self> {
        let mut file = lfile.into_async();
        let mut buf = vec![0; 56];
        file.read_exact(&mut buf).await;
        let (base_index, segment_id) = Self::parse_header(&buf)?;

        Ok(Self {
            file,
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

        Ok(Self {
            file,
            base_index,
            segment_id,
        })
    }

    /// Convert this into the inner tokio file
    pub(super) fn into_inner(self) -> TokioFile {
        self.file
    }

    /// Gets the file name of the WAL segment
    fn segment_name(segment_id: u64, log_index: u64) -> String {
        format!("{:016x}-{:016x}", segment_id, log_index)
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
        buf.extend(vec![0; 7]);
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
            || next_field(3) != &[0; 7]
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
