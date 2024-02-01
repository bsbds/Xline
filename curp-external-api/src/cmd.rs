use std::{fmt::Debug, hash::Hash};

use async_trait::async_trait;
use engine::Snapshot;
use prost::DecodeError;
use serde::{de::DeserializeOwned, Serialize};

use crate::{InflightId, LogIndex};

/// Private
mod pri {
    use super::{Debug, DeserializeOwned, Serialize};

    /// Trait bound for thread safety
    #[allow(unreachable_pub)]
    pub trait ThreadSafe: Debug + Send + Sync + 'static {}

    /// Trait bound for serializable
    #[allow(unreachable_pub)]
    pub trait Serializable: ThreadSafe + Clone + Serialize + DeserializeOwned + Unpin {}
}

impl<T> pri::ThreadSafe for T where T: Debug + Send + Sync + 'static {}

impl<T> pri::Serializable for T where
    T: pri::ThreadSafe + Clone + Serialize + DeserializeOwned + Unpin
{
}

/// Command to execute on the server side
#[async_trait]
pub trait Command: pri::Serializable + ConflictCheck + PbCodec {
    /// Error type
    type Error: pri::Serializable + PbCodec + std::error::Error;

    /// K (key) is used to tell confliction
    /// The key can be a single key or a key range
    type K: pri::Serializable + Eq + Hash + ConflictCheck;

    /// Prepare result
    type PR: pri::Serializable;

    /// Execution result
    type ER: pri::Serializable + PbCodec;

    /// After_sync result
    type ASR: pri::Serializable + PbCodec;

    /// Get keys of the command
    fn keys(&self) -> &[Self::K];

    /// Execute the command according to the executor
    #[inline]
    async fn execute<E>(&self, e: &E) -> Result<Self::ER, Self::Error>
    where
        E: CommandExecutor<Self> + Send + Sync,
    {
        <E as CommandExecutor<Self>>::execute(e, self).await
    }

    /// Execute the command after_sync callback
    #[inline]
    async fn after_sync<E>(&self, e: &E, index: LogIndex) -> Result<Self::ASR, Self::Error>
    where
        E: CommandExecutor<Self> + Send + Sync,
    {
        <E as CommandExecutor<Self>>::after_sync(e, self, index).await
    }
}

/// Check conflict of two keys
pub trait ConflictCheck {
    /// check if this keys conflicts with the `other` key
    ///
    /// Return：
    ///     - True: conflict
    ///     - False: not conflict
    fn is_conflict(&self, other: &Self) -> bool;
}

impl ConflictCheck for String {
    #[inline]
    fn is_conflict(&self, other: &Self) -> bool {
        self == other
    }
}

impl ConflictCheck for u32 {
    #[inline]
    fn is_conflict(&self, other: &Self) -> bool {
        self == other
    }
}

/// Command executor which actually executes the command.
/// It is usually defined by the protocol user.
#[async_trait]
pub trait CommandExecutor<C>: pri::ThreadSafe
where
    C: Command,
{
    /// Execute the command
    async fn mock_exe(&self, cmd: &C) -> Result<C::ER, C::Error>;

    /// Execute the command
    async fn execute(&self, cmd: &C) -> Result<C::ER, C::Error>;

    /// Execute the after_sync callback
    async fn mock_as(&self, cmd: &C, index: LogIndex) -> Result<C::ASR, C::Error>;

    /// Execute the after_sync callback
    async fn after_sync(&self, cmd: &C, index: LogIndex) -> Result<C::ASR, C::Error>;

    /// Set the index of the last log entry that has been successfully applied to the command executor
    fn set_last_applied(&self, index: LogIndex) -> Result<(), C::Error>;

    /// Index of the last log entry that has been successfully applied to the command executor
    fn last_applied(&self) -> Result<LogIndex, C::Error>;

    /// Take a snapshot
    async fn snapshot(&self) -> Result<Snapshot, C::Error>;

    /// Reset the command executor using the snapshot or to the initial state if None
    async fn reset(&self, snapshot: Option<(Snapshot, LogIndex)>) -> Result<(), C::Error>;

    /// Trigger the barrier of the given trigger id (based on propose id) and log index.
    fn trigger(&self, id: InflightId, index: LogIndex);
}

/// Codec for encoding and decoding data into/from the Protobuf format
pub trait PbCodec: Sized {
    /// Encode
    fn encode(&self) -> Vec<u8>;
    /// Decode
    ///
    /// # Errors
    ///
    /// Returns an error if decode failed
    fn decode(buf: &[u8]) -> Result<Self, PbSerializeError>;
}

/// Serialize error used in `PbSerialize`
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum PbSerializeError {
    /// Error during decode phase
    #[error("rpc decode error: {0}")]
    RpcDecode(DecodeError),
    /// Error if the required field is empty
    #[error("field is empty after decoded")]
    EmptyField,
}

impl From<DecodeError> for PbSerializeError {
    #[inline]
    fn from(err: DecodeError) -> Self {
        PbSerializeError::RpcDecode(err)
    }
}
