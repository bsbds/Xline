use std::{collections::HashMap, hash::Hash};

use event_listener::Event;
use futures::{stream::FuturesOrdered, Future, FutureExt, StreamExt};
use parking_lot::Mutex;

/// Barrier for id
#[derive(Debug)]
pub struct IdBarrier<Id> {
    /// Barriers of id
    barriers: Mutex<HashMap<Id, Event>>,
}

impl<Id> IdBarrier<Id> {
    /// Create a new id barrier
    pub fn new() -> Self {
        Self {
            barriers: Mutex::new(HashMap::new()),
        }
    }
}

impl<Id> IdBarrier<Id>
where
    Id: Eq + Hash,
{
    /// Wait for the id until it is triggered.
    pub async fn wait(&self, id: Id) {
        let listener = self
            .barriers
            .lock()
            .entry(id)
            .or_insert_with(Event::new)
            .listen();
        listener.await;
    }

    /// Wait for a collection of ids.
    pub fn wait_all(&self, ids: Vec<Id>) -> impl Future<Output = ()> {
        let mut barriers_l = self.barriers.lock();
        let listeners: FuturesOrdered<_> = ids
            .into_iter()
            .map(|id| barriers_l.entry(id).or_insert_with(Event::new).listen())
            .collect();
        listeners.collect::<Vec<_>>().map(|_| ())
    }

    /// Trigger the barrier of the given inflight id.
    pub fn trigger(&self, id: Id) {
        if let Some(event) = self.barriers.lock().remove(&id) {
            let _ignore = event.notify(usize::MAX);
        }
    }
}
