use std::{
    collections::{HashMap, HashSet},
    io,
    path::Path,
    sync::Arc,
};

use clippy_utilities::OverflowArithmetic;
use curp_external_api::cmd::Command;
use parking_lot::Mutex;
use serde::{de::DeserializeOwned, Serialize};
use sha2::Sha256;
use utils::wal::{get_file_paths_with_ext, pipeline::FilePipeline, LockedFile};

use crate::rpc::ProposeId;

use self::{
    codec::DataFrame,
    config::WALConfig,
    error::WALError,
    segment::{Segment, SegmentAttr},
};

/// WAL codec
mod codec;

/// WAL error
mod error;

/// WAL config
mod config;

/// WAL segment
mod segment;

/// WAL Result
type Result<T> = std::result::Result<T, WALError>;

/// Codec of this WAL
type WALCODEC<C> = codec::WAL<C, Sha256>;

/// Operations of speculative pool WAL
pub(crate) trait PoolWALOps<C: Command> {
    /// Insert a command to WAL
    fn insert(&self, propose_id: ProposeId, cmd: Arc<C>) -> io::Result<()>;

    /// Removes a command from WAL
    fn remove(&self, propose_id: ProposeId) -> io::Result<()>;

    /// Recover all commands stored in WAL
    fn recover(&self) -> io::Result<Vec<C>>;
}

/// WAL of speculative pool
struct SpeculativePoolWAL<C> {
    /// WAL config
    config: WALConfig,
    /// Insert WAL
    insert: Mutex<WAL<segment::Insert, C>>,
    /// Remove WAL
    remove: Mutex<WAL<segment::Remove, C>>,
}

impl<C> SpeculativePoolWAL<C>
where
    C: Serialize + DeserializeOwned,
{
    fn new(config: WALConfig) -> io::Result<Self> {
        let mut insert_dir = config.dir.clone();
        let mut remove_dir = config.dir.clone();
        insert_dir.push("insert");
        remove_dir.push("remove");
        if !config.dir.try_exists()? {
            std::fs::create_dir_all(&config.dir)?;
        }
        if !insert_dir.try_exists()? {
            std::fs::create_dir(&config.dir)?;
        }
        if !remove_dir.try_exists()? {
            std::fs::create_dir(&config.dir)?;
        }
        Ok(Self {
            insert: Mutex::new(WAL::new(insert_dir, config.max_insert_segment_size)?),
            remove: Mutex::new(WAL::new(remove_dir, config.max_remove_segment_size)?),
            config,
        })
    }
}

impl<C: Command> PoolWALOps<C> for SpeculativePoolWAL<C> {
    fn insert(&self, propose_id: ProposeId, cmd: Arc<C>) -> io::Result<()> {
        todo!()
    }

    fn remove(&self, propose_id: ProposeId) -> io::Result<()> {
        todo!()
    }

    fn recover(&self) -> io::Result<Vec<C>> {
        todo!()
    }
}

struct WAL<T, C> {
    /// WAL segments
    segments: Vec<Segment<T, WALCODEC<C>>>,
    /// The pipeline that pre-allocates files
    // TODO: Fix conflict
    pipeline: FilePipeline,
    /// Next segment id
    next_segment_id: u64,
    /// The maximum size of this segment
    max_segment_size: u64,
}

impl<T, C> WAL<T, C>
where
    T: SegmentAttr,
    C: Serialize + DeserializeOwned,
{
    fn new(dir: impl AsRef<Path>, max_segment_size: u64) -> io::Result<Self> {
        Ok(Self {
            segments: Vec::new(),
            pipeline: FilePipeline::new(dir.as_ref().into(), max_segment_size)?,
            next_segment_id: 0,
            max_segment_size,
        })
    }

    fn write_sync(&mut self, item: Vec<DataFrame<C>>) -> io::Result<()> {
        let last_segment = self
            .segments
            .last_mut()
            .unwrap_or_else(|| unreachable!("there should be at least on segment"));
        last_segment.write_sync(item)?;

        if last_segment.is_full() {
            self.open_new_segment()?;
        }

        Ok(())
    }

    fn recover(&mut self, dir: impl AsRef<Path>) -> Result<Vec<DataFrame<C>>> {
        let paths = get_file_paths_with_ext(dir, &segment::Insert::ext())?;
        let lfiles: Vec<_> = paths
            .into_iter()
            .map(LockedFile::open_rw)
            .collect::<io::Result<_>>()?;
        let mut segments: Vec<_> = lfiles
            .into_iter()
            .map(|f| Segment::open(f, self.max_segment_size, WALCODEC::new(), T::r#type()))
            .collect::<Result<_>>()?;

        let logs: Vec<_> = segments
            .iter_mut()
            .map(Segment::recover::<C>)
            .map(|result| result.map_err(Into::into))
            .collect::<io::Result<_>>()?;

        segments.sort_unstable();
        self.next_segment_id = segments.last().map(Segment::segment_id).unwrap_or(0);
        self.segments = segments;

        Ok(logs.into_iter().flatten().collect())
    }

    /// Opens a new WAL segment
    fn open_new_segment(&mut self) -> io::Result<()> {
        let lfile = self
            .pipeline
            .next()
            .ok_or(io::Error::from(io::ErrorKind::BrokenPipe))??;

        let segment = Segment::create(
            lfile,
            self.next_segment_id,
            self.max_segment_size,
            WALCODEC::new(),
            T::r#type(),
        )?;

        self.segments.push(segment);
        self.next_segment_id = self.next_segment_id.overflow_add(1);

        Ok(())
    }
}

impl<C> WAL<segment::Insert, C>
where
    C: Serialize + DeserializeOwned,
{
    fn recover_insert(&mut self, dir: impl AsRef<Path>) -> Result<HashMap<ProposeId, Arc<C>>> {
        Ok(self
            .recover(dir)?
            .into_iter()
            .filter_map(|frame| match frame {
                DataFrame::Insert { propose_id, cmd } => Some((propose_id, cmd)),
                DataFrame::Remove(_) => None,
            })
            .collect())
    }
}

impl<C> WAL<segment::Remove, C>
where
    C: Serialize + DeserializeOwned,
{
    fn recover_remove(&mut self, dir: impl AsRef<Path>) -> Result<HashSet<ProposeId>> {
        Ok(self
            .recover(dir)?
            .into_iter()
            .filter_map(|frame| match frame {
                DataFrame::Insert { .. } => None,
                DataFrame::Remove(propose_id) => Some(propose_id),
            })
            .collect())
    }
}
