use std::{
    collections::{BinaryHeap, HashMap},
    sync::{
        atomic::{AtomicI64, Ordering},
        Arc,
    },
};

use clippy_utilities::OverflowArithmetic;
use curp::cmd::ProposeId;
use event_listener::Event;
use parking_lot::Mutex;

/// Waiter for index
#[derive(Debug)]
pub(crate) struct IndexBarrier {
    /// Inner
    inner: Mutex<IndexBarrierInner>,
}

impl IndexBarrier {
    /// Create a new index barrier
    pub(crate) fn new() -> Self {
        IndexBarrier {
            inner: Mutex::new(IndexBarrierInner {
                next: 1,
                indices: BinaryHeap::new(),
                barriers: HashMap::new(),
                latest_rev: 1,
            }),
        }
    }

    /// Wait for the index until it is triggered.
    pub(crate) async fn wait(&self, index: u64) -> i64 {
        if index == 0 {
            return 0;
        }
        let (listener, revision) = {
            let mut inner_l = self.inner.lock();
            if inner_l.next > index {
                return inner_l.latest_rev;
            }
            let Trigger { event, revision } = inner_l
                .barriers
                .entry(index)
                .or_insert_with(Trigger::default);
            (event.listen(), Arc::clone(&revision))
        };
        listener.await;
        revision.load(Ordering::SeqCst)
    }

    /// Trigger all barriers whose index is less than or equal to the given index.
    pub(crate) fn trigger(&self, index: u64, revision: i64) {
        let mut inner_l = self.inner.lock();
        inner_l.indices.push(IndexRevision::new(index, revision));
        while inner_l
            .indices
            .peek()
            .map_or(false, |i| i.index.eq(&inner_l.next))
        {
            let next = inner_l.next;
            let IndexRevision { index: _, revision } = inner_l
                .indices
                .pop()
                .unwrap_or_else(|| unreachable!("IndexRevision should be Some"));
            inner_l.latest_rev = revision;
            if let Some(trigger) = inner_l.barriers.remove(&next) {
                trigger.revision.store(revision, Ordering::SeqCst);
                trigger.event.notify(usize::MAX);
            }
            inner_l.next = next.overflow_add(1);
        }
    }
}

/// Inner of index barrier.
#[derive(Debug)]
struct IndexBarrierInner {
    next: u64,
    indices: BinaryHeap<IndexRevision>,
    barriers: HashMap<u64, Trigger>,
    /// latest revision of the last triggered log index
    latest_rev: i64,
}

/// Barrier for id
#[derive(Debug)]
pub(crate) struct IdBarrier {
    /// Barriers of id
    barriers: Mutex<HashMap<ProposeId, Trigger>>,
}

impl IdBarrier {
    /// Create a new id barrier
    pub(crate) fn new() -> Self {
        Self {
            barriers: Mutex::new(HashMap::new()),
        }
    }

    /// Wait for the id until it is triggered.
    pub(crate) async fn wait(&self, id: ProposeId) -> i64 {
        let (listener, revision) = {
            let mut barriers_l = self.barriers.lock();
            let Trigger { event, revision } = barriers_l.entry(id).or_insert_with(Trigger::default);
            (event.listen(), Arc::clone(&revision))
        };
        listener.await;
        revision.load(Ordering::SeqCst)
    }

    /// Trigger the barrier of the given id.
    pub(crate) fn trigger(&self, id: &ProposeId, rev: i64) {
        if let Some(trigger) = self.barriers.lock().remove(id) {
            let Trigger { event, revision } = trigger;
            revision.store(rev, Ordering::SeqCst);
            event.notify(usize::MAX);
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
struct IndexRevision {
    index: u64,
    revision: i64,
}

impl IndexRevision {
    fn new(index: u64, revision: i64) -> Self {
        Self { index, revision }
    }
}

impl PartialOrd for IndexRevision {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        other.index.partial_cmp(&self.index)
    }
}

impl Ord for IndexRevision {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        other.index.cmp(&self.index)
    }
}

#[derive(Debug, Default)]
struct Trigger {
    event: Event,
    revision: Arc<AtomicI64>,
}

#[cfg(test)]
mod test {
    use std::{sync::Arc, time::Duration};

    use futures::future::join_all;
    use test_macros::abort_on_panic;
    use tokio::time::{sleep, timeout};

    use super::*;

    #[tokio::test]
    #[abort_on_panic]
    async fn test_id_barrier() {
        let id_barrier = Arc::new(IdBarrier::new());
        let barriers = (0..5)
            .map(|i| {
                let id_barrier = Arc::clone(&id_barrier);
                tokio::spawn(async move {
                    id_barrier.wait(i.to_string()).await;
                })
            })
            .collect::<Vec<_>>();
        sleep(Duration::from_millis(10)).await;
        for i in 0..5 {
            id_barrier.trigger(&i.to_string(), 0);
        }
        timeout(Duration::from_millis(100), join_all(barriers))
            .await
            .unwrap();
    }

    #[tokio::test]
    #[abort_on_panic]
    async fn test_index_barrier() {
        let index_barrier = Arc::new(IndexBarrier::new());
        let (done_tx, done_rx) = flume::bounded(5);
        let barriers = (1..=5)
            .map(|i| {
                let index_barrier = Arc::clone(&index_barrier);
                let done_tx_c = done_tx.clone();
                tokio::spawn(async move {
                    let rev = index_barrier.wait(i).await;
                    done_tx_c.send(rev).unwrap();
                })
            })
            .collect::<Vec<_>>();

        index_barrier.trigger(2, 2);
        index_barrier.trigger(3, 3);
        sleep(Duration::from_millis(100)).await;
        assert!(done_rx.try_recv().is_err());
        index_barrier.trigger(1, 1);
        sleep(Duration::from_millis(100)).await;
        assert_eq!(done_rx.try_recv().unwrap(), Some(1));
        assert_eq!(done_rx.try_recv().unwrap(), Some(2));
        assert_eq!(done_rx.try_recv().unwrap(), Some(3));
        index_barrier.trigger(4, 4);
        index_barrier.trigger(5, 5);

        assert_eq!(
            timeout(Duration::from_millis(100), index_barrier.wait(3))
                .await
                .unwrap(),
            Some(3)
        );

        timeout(Duration::from_millis(100), join_all(barriers))
            .await
            .unwrap();
    }
}
