use curp_external_api::conflict::SpeculativePool;

use crate::rpc::PoolEntry;

use super::{CommandEntry, ConfChangeEntry, SplitEntry};

/// A speculative pool object
pub type SpObject<C> = Box<dyn SpeculativePool<Entry = CommandEntry<C>> + Send + 'static>;

/// Union type of `SpeculativePool` objects
pub(crate) struct SpecPool<C> {
    /// Command speculative pools
    command_sps: Vec<SpObject<C>>,
    /// Conf change speculative pool
    conf_change_sp: ConfChangeSpecPool,
}

impl<C> SpecPool<C> {
    /// Creates a new pool
    pub(crate) fn new(command_sps: Vec<SpObject<C>>) -> Self {
        Self {
            command_sps,
            conf_change_sp: ConfChangeSpecPool::default(),
        }
    }

    /// Inserts an entry into the pool
    pub(crate) fn insert(&mut self, entry: PoolEntry<C>) -> Option<PoolEntry<C>> {
        if !self.conf_change_sp.is_empty() {
            return Some(entry);
        }

        match SplitEntry::from(entry) {
            SplitEntry::Command(c) => {
                for csp in &mut self.command_sps {
                    if let Some(e) = csp.insert(c.clone()) {
                        return Some(e.into());
                    }
                }
            }
            SplitEntry::ConfChange(c) => {
                if !self
                    .command_sps
                    .iter()
                    .map(AsRef::as_ref)
                    .all(SpeculativePool::is_empty)
                {
                    return Some(c.into());
                }
                let _ignore = self.conf_change_sp.insert(c);
            }
        }

        None
    }

    // TODO: Use reference instead of clone
    /// Removes an entry from the pool
    pub(crate) fn remove(&mut self, entry: PoolEntry<C>) {
        match SplitEntry::from(entry) {
            SplitEntry::Command(c) => {
                for csp in &mut self.command_sps {
                    csp.remove(c.clone());
                }
            }
            SplitEntry::ConfChange(c) => {
                self.conf_change_sp.remove(c);
            }
        }
    }

    /// Returns all entries in the pool
    pub(crate) fn all(&self) -> Vec<PoolEntry<C>> {
        let mut entries = Vec::new();
        for csp in &self.command_sps {
            entries.extend(csp.all().into_iter().map(Into::into));
        }
        entries.extend(self.conf_change_sp.all().into_iter().map(Into::into));
        entries
    }

    /// Returns the number of entries in the pool
    #[allow(clippy::arithmetic_side_effects)] // Pool sizes can't overflow a `usize`
    pub(crate) fn len(&self) -> usize {
        self.command_sps
            .iter()
            .fold(0, |sum, pool| sum + pool.len())
            + self.conf_change_sp.len()
    }
}

/// Speculative pool for conf change entries
#[derive(Default)]
struct ConfChangeSpecPool {
    /// Store current conf change
    change: Option<ConfChangeEntry>,
}

impl SpeculativePool for ConfChangeSpecPool {
    type Entry = ConfChangeEntry;

    fn insert(&mut self, entry: Self::Entry) -> Option<Self::Entry> {
        if self.change.is_some() {
            return Some(entry);
        }
        self.change = Some(entry);
        None
    }

    fn is_empty(&self) -> bool {
        self.change.is_none()
    }

    fn remove(&mut self, _entry: Self::Entry) {
        self.change = None;
    }

    fn all(&self) -> Vec<Self::Entry> {
        self.change.clone().into_iter().collect()
    }

    fn clear(&mut self) {
        self.change = None;
    }

    fn len(&self) -> usize {
        self.change.iter().count()
    }
}
