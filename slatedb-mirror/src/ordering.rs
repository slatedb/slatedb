//! Per-path ordering of downloads, writes, deletes, and evictions.
//!
//! Each path has a FIFO queue of stages. A stage runs once it reaches the
//! front and leaves the queue when every ticket in it has been dropped.
//!
//! - Adjacent downloads (`fetch`, `prefetch`, `Refetch`) share one stage and
//!   run together. The download engine's single-flight dedupes them.
//! - A download or write cancels any evictions queued behind the last
//!   non-eviction stage that haven't started. An eviction that has started
//!   (is at the front) is waited for.
//! - An eviction queued behind another eviction is dropped as redundant.
//!
//! Registration is synchronous, so an operation's place in line is fixed when
//! it's issued rather than when its future is first polled.

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

use object_store::path::Path;
use parking_lot::Mutex;
use tokio::sync::watch;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum OpKind {
    /// `fetch`, `prefetch`, or a `Refetch` read.
    Download,
    /// A `Mirror` PUT or multipart upload.
    Write,
    /// A DELETE or a remote-scan removal.
    Delete,
    /// An eviction.
    Evict,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Turn {
    /// Queued behind another stage.
    Waiting,
    /// At the front of the queue.
    Run,
    /// Removed from the queue without running.
    Cancelled,
}

#[derive(Debug)]
struct Stage {
    kind: OpKind,
    turn: watch::Sender<Turn>,
}

#[derive(Debug)]
struct Slot {
    stage: Arc<Stage>,
    /// Live tickets for this stage.
    tickets: usize,
}

type Queues = Mutex<HashMap<Path, VecDeque<Slot>>>;

/// The per-path queues for one mirror.
#[derive(Debug, Default)]
pub(crate) struct PathOrdering {
    queues: Arc<Queues>,
}

impl PathOrdering {
    /// Queues an operation on `path`. Returns `None` for an eviction that
    /// would be redundant with one already queued.
    pub(crate) fn register(&self, path: &Path, kind: OpKind) -> Option<Ticket> {
        let mut queues = self.queues.lock();
        let queue = queues.entry(path.clone()).or_default();
        match kind {
            OpKind::Evict => {
                if queue
                    .back()
                    .is_some_and(|slot| slot.stage.kind == OpKind::Evict)
                {
                    return None;
                }
            }
            OpKind::Download | OpKind::Write => {
                // Cancel evictions that haven't started. The front stage has
                // started, so it's never removed here.
                while queue.len() > 1
                    && queue
                        .back()
                        .is_some_and(|slot| slot.stage.kind == OpKind::Evict)
                {
                    let slot = queue.pop_back().expect("queue is not empty");
                    slot.stage.turn.send_replace(Turn::Cancelled);
                }
                if kind == OpKind::Download {
                    if let Some(slot) = queue
                        .back_mut()
                        .filter(|slot| slot.stage.kind == OpKind::Download)
                    {
                        slot.tickets += 1;
                        return Some(Ticket {
                            queues: Arc::clone(&self.queues),
                            path: path.clone(),
                            stage: Arc::clone(&slot.stage),
                        });
                    }
                }
            }
            OpKind::Delete => {}
        }

        let turn = if queue.is_empty() {
            Turn::Run
        } else {
            Turn::Waiting
        };
        let stage = Arc::new(Stage {
            kind,
            turn: watch::Sender::new(turn),
        });
        queue.push_back(Slot {
            stage: Arc::clone(&stage),
            tickets: 1,
        });
        Some(Ticket {
            queues: Arc::clone(&self.queues),
            path: path.clone(),
            stage,
        })
    }

    #[cfg(test)]
    pub(crate) fn is_empty(&self) -> bool {
        self.queues.lock().is_empty()
    }
}

/// A place in a path's queue. Dropping it lets the stages behind it run once
/// the rest of its stage is done.
#[derive(Debug)]
pub(crate) struct Ticket {
    queues: Arc<Queues>,
    path: Path,
    stage: Arc<Stage>,
}

impl Ticket {
    /// Waits for this ticket's turn. Returns false if the operation was
    /// cancelled, in which case it must not run.
    pub(crate) async fn ready(&self) -> bool {
        let mut turn = self.stage.turn.subscribe();
        let turn = *turn
            .wait_for(|turn| *turn != Turn::Waiting)
            .await
            .expect("stage owns the sender");
        turn == Turn::Run
    }
}

impl Drop for Ticket {
    fn drop(&mut self) {
        let mut queues = self.queues.lock();
        let Some(queue) = queues.get_mut(&self.path) else {
            return;
        };
        // A cancelled stage has already been removed.
        let Some(index) = queue
            .iter()
            .position(|slot| Arc::ptr_eq(&slot.stage, &self.stage))
        else {
            return;
        };
        queue[index].tickets -= 1;
        if queue[index].tickets > 0 {
            return;
        }
        queue.remove(index);
        if index == 0 {
            if let Some(next) = queue.front() {
                next.stage.turn.send_replace(Turn::Run);
            }
        }
        if queue.is_empty() {
            queues.remove(&self.path);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::task::{Context, Poll};

    use futures::task::noop_waker_ref;

    use super::*;

    fn is_ready(ticket: &Ticket) -> Option<bool> {
        let mut fut = Box::pin(ticket.ready());
        match fut
            .as_mut()
            .poll(&mut Context::from_waker(noop_waker_ref()))
        {
            Poll::Ready(ready) => Some(ready),
            Poll::Pending => None,
        }
    }

    fn path() -> Path {
        Path::from("a/b.sst")
    }

    #[test]
    fn should_run_stages_in_order() {
        let ordering = PathOrdering::default();
        let write = ordering.register(&path(), OpKind::Write).unwrap();
        let delete = ordering.register(&path(), OpKind::Delete).unwrap();
        let other = ordering
            .register(&Path::from("a/c.sst"), OpKind::Write)
            .unwrap();

        assert_eq!(is_ready(&write), Some(true));
        assert_eq!(is_ready(&delete), None);
        assert_eq!(is_ready(&other), Some(true));

        drop(write);
        assert_eq!(is_ready(&delete), Some(true));
        drop(delete);
        drop(other);
        assert!(ordering.is_empty());
    }

    #[test]
    fn should_share_a_stage_between_adjacent_downloads() {
        let ordering = PathOrdering::default();
        let first = ordering.register(&path(), OpKind::Download).unwrap();
        let second = ordering.register(&path(), OpKind::Download).unwrap();
        let evict = ordering.register(&path(), OpKind::Evict).unwrap();
        assert_eq!(is_ready(&first), Some(true));
        assert_eq!(is_ready(&second), Some(true));

        // The eviction waits for both downloads.
        drop(first);
        assert_eq!(is_ready(&evict), None);
        drop(second);
        assert_eq!(is_ready(&evict), Some(true));
    }

    #[test]
    fn should_cancel_pending_eviction() {
        let ordering = PathOrdering::default();
        let delete = ordering.register(&path(), OpKind::Delete).unwrap();
        let evict = ordering.register(&path(), OpKind::Evict).unwrap();
        let download = ordering.register(&path(), OpKind::Download).unwrap();

        assert_eq!(is_ready(&evict), Some(false));
        assert_eq!(is_ready(&download), None);
        drop(evict);
        drop(delete);
        assert_eq!(is_ready(&download), Some(true));
    }

    #[test]
    fn should_wait_for_started_eviction() {
        let ordering = PathOrdering::default();
        let evict = ordering.register(&path(), OpKind::Evict).unwrap();
        assert_eq!(is_ready(&evict), Some(true));

        let write = ordering.register(&path(), OpKind::Write).unwrap();
        assert_eq!(is_ready(&evict), Some(true));
        assert_eq!(is_ready(&write), None);
        drop(evict);
        assert_eq!(is_ready(&write), Some(true));
    }

    #[test]
    fn should_drop_redundant_eviction() {
        let ordering = PathOrdering::default();
        let running = ordering.register(&path(), OpKind::Evict).unwrap();
        assert!(ordering.register(&path(), OpKind::Evict).is_none());

        let delete = ordering.register(&path(), OpKind::Delete).unwrap();
        let pending = ordering.register(&path(), OpKind::Evict).unwrap();
        assert!(ordering.register(&path(), OpKind::Evict).is_none());

        drop(running);
        drop(delete);
        assert_eq!(is_ready(&pending), Some(true));
    }

    #[test]
    fn should_not_block_on_dropped_ticket_behind_running_stage() {
        let ordering = PathOrdering::default();
        let write = ordering.register(&path(), OpKind::Write).unwrap();
        let abandoned = ordering.register(&path(), OpKind::Download).unwrap();
        let delete = ordering.register(&path(), OpKind::Delete).unwrap();

        // Dropping a queued ticket doesn't let later stages jump the one that
        // is running.
        drop(abandoned);
        assert_eq!(is_ready(&delete), None);
        drop(write);
        assert_eq!(is_ready(&delete), Some(true));
    }
}
