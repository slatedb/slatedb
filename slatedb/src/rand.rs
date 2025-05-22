//! SlateDB's module for generating random data.
//!
//! This module exists because we want to do deterministic simulation testing for SlateDB.
//! To do so, we need a way to easily seed all random number generators in the library in a
//! deterministic way.
//!
//! To do this, we use a global random number generator that is initialized with a seed.
//!
//! Each thread also has its own random number generator that is initialized from the global
//! random number generator. This ensures that each thread has its own random number generator
//! that is initialized deterministically from the global random number generator. This is useful
//! for deterministic simulation testing. We can seed the root RNG and have all random numbers
//! derive from that without having to share the root RNG across threads, which would create a
//! point of contention.
//!
//! ## Usage
//!
//! ```rust
//! use slate_db::rand::seed;
//!
//! seed(42);
//! let _ = with_rng(|rng| rng.u64(..));
//! ```

use fastrand::Rng;
use std::cell::RefCell;
use std::sync::{Mutex, OnceLock};

use crate::SlateDBError;

// A global random number generator used for all randomness in SlateDB.
//
// Uses `OnceLock` to ensure that the random number generator is initialized exactly once regardless
// of which thread it was initialized on. Uses `Mutex` so we can grab a mutable reference to the
// random number generator when forking the root RNG in a local thread.
static ROOT_RNG: OnceLock<Mutex<Rng>> = OnceLock::new();

thread_local! {
    // A thread-local random number generator used for all randomness in a single thread. Not
    // threadsafe.
    static THREAD_RNG: RefCell<Option<Rng>> = RefCell::new(None);
}

/// Initialize the global random number generator with a seed. This should only be called once
/// upon start.
///
/// ## Errors
///
/// Returns an error if the random number generator has already been initialized.
pub(crate) fn seed(seed: u64) -> Result<(), SlateDBError> {
    ROOT_RNG
        .set(Mutex::new(Rng::with_seed(seed)))
        .map_err(|_| SlateDBError::InvalidDBState)
}

/// Execute a function with a thread-local random number generator.
///
/// If the thread-local random number generator hasn't been initialized, it will be initialized
/// using a random seed from the global random number generator.
///
/// If the global random number generator hasn't been initialized, a default root random number
/// generator will be created with a random seed.
pub fn with_rng<T>(f: impl FnOnce(&mut Rng) -> T) -> T {
    THREAD_RNG.with(|cell| {
        let mut maybe_rng = cell.borrow_mut();
        if maybe_rng.is_none() {
            let rng = ROOT_RNG
                .get_or_init(|| Mutex::new(Rng::new()))
                .lock()
                .expect("mutex poisoned")
                .fork();
            *maybe_rng = Some(rng);
        }
        f(maybe_rng.as_mut().expect("rng not initialized"))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    // Helper to force a thread-local random number generator to use a specific seed so we
    // can test deterministic behavior.
    fn seed_local(seed: u64) {
        THREAD_RNG.with(|cell| {
            cell.replace(Some(Rng::with_seed(seed)));
        });
    }

    #[test]
    fn test_rng() {
        seed_local(42);
        let rand_u64 = with_rng(|rng| rng.u64(..));
        assert_eq!(rand_u64, 14587678697106979209);
    }

    #[test]
    fn test_rng_thread_local() {
        seed_local(42);
        with_rng(|outer_rng| {
            let outer_u64 = outer_rng.u64(..);
            let inner_u64 = std::thread::spawn(move || {
                seed_local(64);
                with_rng(|inner_rng| inner_rng.u64(..))
            })
            .join()
            .unwrap();
            assert_eq!(inner_u64, 13858685378923336244);
            assert_eq!(outer_u64, 14587678697106979209);
        });
    }

    #[test]
    fn test_rng_lazy_init() {
        std::thread::spawn(move || {
            assert!(THREAD_RNG.with(|c| c.borrow().is_none()));
            with_rng(|_| {});
            assert!(THREAD_RNG.with(|c| c.borrow().is_some()));
        })
        .join()
        .unwrap();
    }
}
