use std::{fmt::Debug, sync::Arc};

use futures::channel::mpsc::{channel, Sender};
use tonic::transport::Channel;
use xline::server::KeyRange;
use xlineapi::{self, RequestUnion, WatchCancelRequest, WatchProgressRequest, WatchResponse};

use crate::{
    error::{ClientError, Result},
    AuthService,
};

/// The maintenance client
#[derive(Clone, Debug)]
pub struct WatchClient {
    /// The watch RPC client, only communicate with one server at a time
    inner: xlineapi::WatchClient<AuthService<Channel>>,
}

impl WatchClient {
    /// Create a new maintenance client
    #[inline]
    #[must_use]
    pub fn new(channel: Channel, token: Option<String>) -> Self {
        Self {
            inner: xlineapi::WatchClient::new(AuthService::new(
                channel,
                token.and_then(|t| t.parse().ok().map(Arc::new)),
            )),
        }
    }

    /// Watches for events happening or that have happened. Both input and output
    /// are streams; the input stream is for creating and canceling watcher and the output
    /// stream sends events. The entire event history can be watched starting from the
    /// last compaction revision.
    ///
    /// # Errors
    ///
    /// If the RPC client fails to send request
    ///
    /// # Panics
    ///
    /// If the RPC server returns the wrong value
    #[inline]
    pub async fn watch(
        &mut self,
        request: WatchRequest,
    ) -> Result<(Watcher, tonic::Streaming<WatchResponse>)> {
        let (mut request_sender, request_receiver) = channel::<xlineapi::WatchRequest>(100);

        let request = xlineapi::WatchRequest {
            request_union: Some(RequestUnion::CreateRequest(request.into())),
        };

        request_sender
            .try_send(request)
            .map_err(|e| ClientError::WatchError(e.to_string()))?;

        let mut response_stream = self.inner.watch(request_receiver).await?.into_inner();

        let watch_id = match response_stream.message().await? {
            Some(resp) => {
                assert!(resp.created, "not a create watch response");
                resp.watch_id
            }
            None => {
                return Err(ClientError::WatchError(String::from(
                    "failed to create watch",
                )));
            }
        };

        Ok((Watcher::new(watch_id, request_sender), response_stream))
    }
}

/// The watching handle.
#[derive(Debug)]
pub struct Watcher {
    /// Id of the watcher
    watch_id: i64,
    /// The channel sender
    sender: Sender<xlineapi::WatchRequest>,
}

impl Watcher {
    /// Creates a new `Watcher`.
    #[inline]
    const fn new(watch_id: i64, sender: Sender<xlineapi::WatchRequest>) -> Self {
        Self { watch_id, sender }
    }

    /// The ID of the watcher.
    #[inline]
    #[must_use]
    pub const fn watch_id(&self) -> i64 {
        self.watch_id
    }

    /// Watches for events happening or that have happened.
    ///
    /// # Errors
    ///
    /// If sender fails to send to channel
    #[inline]
    pub fn watch(&mut self, request: WatchRequest) -> Result<()> {
        let request = xlineapi::WatchRequest {
            request_union: Some(RequestUnion::CreateRequest(request.into())),
        };

        self.sender
            .try_send(request)
            .map_err(|e| ClientError::WatchError(e.to_string()))
    }

    /// Cancels this watcher.
    ///
    /// # Errors
    ///
    /// If sender fails to send to channel
    #[inline]
    pub fn cancel(&mut self) -> Result<()> {
        let request = xlineapi::WatchRequest {
            request_union: Some(RequestUnion::CancelRequest(WatchCancelRequest {
                watch_id: self.watch_id,
            })),
        };

        self.sender
            .try_send(request)
            .map_err(|e| ClientError::WatchError(e.to_string()))
    }

    /// Cancels watch by specified `watch_id`.
    ///
    /// # Errors
    ///
    /// If sender fails to send to channel
    #[inline]
    pub fn cancel_by_id(&mut self, watch_id: i64) -> Result<()> {
        let request = xlineapi::WatchRequest {
            request_union: Some(RequestUnion::CancelRequest(WatchCancelRequest { watch_id })),
        };

        self.sender
            .try_send(request)
            .map_err(|e| ClientError::WatchError(e.to_string()))
    }

    /// Requests a watch stream progress status be sent in the watch response stream as soon as
    /// possible.
    ///
    /// # Errors
    ///
    /// If sender fails to send to channel
    #[inline]
    pub fn request_progress(&mut self) -> Result<()> {
        let request = xlineapi::WatchRequest {
            request_union: Some(RequestUnion::ProgressRequest(WatchProgressRequest {})),
        };

        self.sender
            .try_send(request)
            .map_err(|e| ClientError::WatchError(e.to_string()))
    }
}

/// Watch Request
#[derive(Clone, Debug, PartialEq)]
pub struct WatchRequest {
    /// inner watch create request
    inner: xlineapi::WatchCreateRequest,
}

impl WatchRequest {
    /// New `WatchRequest`
    #[inline]
    #[must_use]
    pub fn new(key: impl Into<Vec<u8>>) -> Self {
        Self {
            inner: xlineapi::WatchCreateRequest {
                key: key.into(),
                ..Default::default()
            },
        }
    }

    /// Set `key` and `range_end` when with prefix
    #[inline]
    #[must_use]
    pub fn with_prefix(mut self) -> Self {
        if self.inner.key.is_empty() {
            self.inner.key = vec![0];
            self.inner.range_end = vec![0];
        } else {
            self.inner.range_end = KeyRange::get_prefix(&self.inner.key);
        }
        self
    }

    /// Set `key` and `range_end` when with from key
    #[inline]
    #[must_use]
    pub fn with_from_key(mut self) -> Self {
        if self.inner.key.is_empty() {
            self.inner.key = vec![0];
        }
        self.inner.range_end = vec![0];
        self
    }

    /// Set `range_end`
    #[inline]
    #[must_use]
    pub fn with_range_end(mut self, range_end: impl Into<Vec<u8>>) -> Self {
        self.inner.range_end = range_end.into();
        self
    }

    /// Sets the revision to watch from (inclusive). No `start_revision` is "now".
    #[inline]
    #[must_use]
    pub const fn with_start_revision(mut self, revision: i64) -> Self {
        self.inner.start_revision = revision;
        self
    }

    /// `progress_notify` is set so that the etcd server will periodically send a `WatchResponse` with
    /// no events to the new watcher if there are no recent events. It is useful when clients
    /// wish to recover a disconnected watcher starting from a recent known revision.
    /// The etcd server may decide how often it will send notifications based on current load.
    #[inline]
    #[must_use]
    pub const fn with_progress_notify(mut self) -> Self {
        self.inner.progress_notify = true;
        self
    }

    /// Filter the events at server side before it sends back to the watcher.
    #[inline]
    #[must_use]
    pub fn with_filters(mut self, filters: impl Into<Vec<WatchFilterType>>) -> Self {
        self.inner.filters = filters.into().into_iter().map(Into::into).collect();
        self
    }

    /// If `prev_kv` is set, created watcher gets the previous KV before the event happens.
    /// If the previous KV is already compacted, nothing will be returned.
    #[inline]
    #[must_use]
    pub const fn with_prev_kv(mut self) -> Self {
        self.inner.prev_kv = true;
        self
    }

    /// If `watch_id` is provided and non-zero, it will be assigned to this watcher.
    /// Since creating a watcher in etcd is not a synchronous operation,
    /// this can be used ensure that ordering is correct when creating multiple
    /// watchers on the same stream. Creating a watcher with an ID already in
    /// use on the stream will cause an error to be returned.
    #[inline]
    #[must_use]
    pub const fn with_watch_id(mut self, watch_id: i64) -> Self {
        self.inner.watch_id = watch_id;
        self
    }

    /// Enables splitting large revisions into multiple watch responses.
    #[inline]
    #[must_use]
    pub const fn with_fragment(mut self) -> Self {
        self.inner.fragment = true;
        self
    }
}

impl From<WatchRequest> for xlineapi::WatchCreateRequest {
    #[inline]
    fn from(request: WatchRequest) -> Self {
        request.inner
    }
}

/// Watch filter type.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[non_exhaustive]
pub enum WatchFilterType {
    /// Filter out put event.
    NoPut = 0,
    /// Filter out delete event.
    NoDelete = 1,
}

impl From<WatchFilterType> for i32 {
    #[inline]
    fn from(value: WatchFilterType) -> Self {
        match value {
            WatchFilterType::NoPut => 0,
            WatchFilterType::NoDelete => 1,
        }
    }
}
