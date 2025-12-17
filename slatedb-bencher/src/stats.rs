//! Generic statistics recording with rolling time windows.
//!
//! This module provides a generic stats recorder that can be used by different
//! benchmarks (db, transaction, etc.) without duplicating the windowing logic.

use std::collections::VecDeque;
use std::ops::Range;
use std::sync::Mutex;
use std::time::Duration;
use tokio::time::Instant;

/// The size of each time window for stats collection.
pub const WINDOW_SIZE: Duration = Duration::from_secs(10);

/// Maximum number of windows to keep in history.
const MAX_WINDOWS: usize = 180;

/// Trait for stats that can be stored in a time window.
pub trait WindowStats: Default {
    /// Get the time range for this window.
    fn range(&self) -> Range<Instant>;

    /// Set the time range for this window.
    fn set_range(&mut self, range: Range<Instant>);
}

/// Generic stats recorder with rolling time windows.
pub struct StatsRecorder<T: WindowStats> {
    inner: Mutex<StatsRecorderInner<T>>,
}

struct StatsRecorderInner<T: WindowStats> {
    windows: VecDeque<T>,
}

impl<T: WindowStats> StatsRecorder<T> {
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(StatsRecorderInner {
                windows: VecDeque::new(),
            }),
        }
    }

    /// Record stats by applying an update function to the current window.
    pub fn record<F>(&self, now: Instant, update_fn: F)
    where
        F: FnOnce(&mut T),
    {
        let mut guard = self.inner.lock().expect("lock failed");
        Self::maybe_roll_window(now, &mut guard.windows);
        if let Some(front) = guard.windows.front_mut() {
            update_fn(front);
        }
    }

    /// Get stats since a lookback duration by applying a fold function to windows.
    pub fn stats_since<R, F>(&self, lookback: Duration, fold_fn: F) -> Option<R>
    where
        F: FnOnce(Range<Instant>, Vec<&T>) -> R,
    {
        let guard = self.inner.lock().expect("lock failed");
        Self::sum_windows(&guard.windows, lookback, fold_fn)
    }

    /// Rolls the window if necessary. Creates a new window if there are no windows.
    fn maybe_roll_window(now: Instant, windows: &mut VecDeque<T>) {
        let Some(front) = windows.front() else {
            let mut new_window = T::default();
            new_window.set_range(now..now + WINDOW_SIZE);
            windows.push_front(new_window);
            return;
        };

        let mut front_range = front.range();
        while now >= front_range.end {
            let mut new_window = T::default();
            new_window.set_range(front_range.end..front_range.end + WINDOW_SIZE);
            windows.push_front(new_window);

            while windows.len() > MAX_WINDOWS {
                windows.pop_back();
            }

            front_range = windows.front().unwrap().range();
        }
    }

    /// Sums the stats in the windows contained in the lookback period.
    fn sum_windows<R, F>(windows: &VecDeque<T>, lookback: Duration, fold_fn: F) -> Option<R>
    where
        F: FnOnce(Range<Instant>, Vec<&T>) -> R,
    {
        let mut windows_iter = windows.iter();

        // Don't count the active window, but use its start point as the end of the range.
        let active_window = windows_iter.next()?;
        let range_end = active_window.range().start;

        // Collect windows within the lookback period
        let relevant_windows: Vec<&T> = windows_iter
            .filter(|w| w.range().start >= range_end - lookback)
            .collect();

        if relevant_windows.is_empty() {
            return None;
        }

        let range_start = relevant_windows.last().unwrap().range().start;
        let range = range_start..range_end;

        Some(fold_fn(range, relevant_windows))
    }
}

impl<T: WindowStats> Default for StatsRecorder<T> {
    fn default() -> Self {
        Self::new()
    }
}
