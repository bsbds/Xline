//                LEASE_KEEP_ALIVE
//                       |
// KV_UPDATES      TONIC_SERVER       ELECTION
//       \        /      |      \       /
//      WATCH_TASK  CONF_CHANGE  LOG_PERSIST

/// Generate enum with iterator
macro_rules! enum_with_iter {
    ( $($variant:ident),* $(,)? ) => {
        /// Task name
        #[derive(Debug, Copy, Clone, Hash, Eq, PartialEq)]
        #[non_exhaustive]
        #[allow(missing_docs)]
        pub enum TaskName {
            $($variant),*
        }

        impl TaskName {
            /// Get iter of all task names
            #[inline]
            pub fn iter() -> impl Iterator<Item = TaskName> {
                static VARIANTS: &'static [TaskName] = &[
                    $(TaskName::$variant),*
                ];
                VARIANTS.iter().copied()
            }
        }
    }
}
enum_with_iter! {
    CompactBg,
    KvUpdates,
    WatchTask,
    LeaseKeepAlive,
    TonicServer,
    LogPersist,
    Election,
    SyncFollower,
    ConfChange,
    GcSpecPool,
    GcCmdBoard,
    RevokeExpiredLeases,
    SyncVictims,
    AutoCompactor,
    AfterSync,
    HandlePropose,
}

/// All edges of task graph, the first item in each pair must be shut down before the second item
pub const ALL_EDGES: [(TaskName, TaskName); 6] = [
    (TaskName::KvUpdates, TaskName::WatchTask),
    (TaskName::LeaseKeepAlive, TaskName::TonicServer),
    (TaskName::TonicServer, TaskName::WatchTask),
    (TaskName::TonicServer, TaskName::ConfChange),
    (TaskName::TonicServer, TaskName::LogPersist),
    (TaskName::Election, TaskName::LogPersist),
];
