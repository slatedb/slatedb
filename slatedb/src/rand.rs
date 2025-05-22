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

use rand_xoshiro::rand_core::{RngCore, SeedableRng};
use rand_xoshiro::Xoroshiro128Plus;
use std::cell::RefCell;
use std::sync::{Mutex, OnceLock};

use crate::SlateDBError;

type RngAlg = Xoroshiro128Plus;

// A global random number generator used for all randomness in SlateDB.
//
// Uses `OnceLock` to ensure that the random number generator is initialized exactly once regardless
// of which thread it was initialized on. Uses `Mutex` so we can grab a mutable reference to the
// random number generator when forking the root RNG in a local thread.
static ROOT_RNG: OnceLock<Mutex<RngAlg>> = OnceLock::new();

thread_local! {
    // A thread-local random number generator used for all randomness in a single thread. Not
    // threadsafe.
    static THREAD_RNG: RefCell<Option<RngAlg>> = const { RefCell::new(None) };
}

/// Initialize the global random number generator with a seed. This should only be called once
/// upon start.
///
/// ## Errors
///
/// Returns an error if the random number generator has already been initialized.
pub(crate) fn seed(seed: u64) -> Result<(), SlateDBError> {
    ROOT_RNG
        .set(Mutex::new(RngAlg::seed_from_u64(seed)))
        .map_err(|_| SlateDBError::InvalidDBState)
}

/// Execute a function with a thread-local random number generator.
///
/// If the thread-local random number generator hasn't been initialized, it will be initialized
/// using a random seed from the global random number generator.
///
/// If the global random number generator hasn't been initialized, a default root random number
/// generator will be created with a random seed.
pub fn with_rng<T>(f: impl FnOnce(&mut dyn RngCore) -> T) -> T {
    THREAD_RNG.with(|cell| {
        let mut maybe_rng = cell.borrow_mut();
        if maybe_rng.is_none() {
            let mut root_rng = ROOT_RNG
                .get_or_init(|| Mutex::new(RngAlg::seed_from_u64(0)))
                .lock()
                .expect("mutex poisoned");
            let thread_local_rng = Xoroshiro128Plus::seed_from_u64(root_rng.next_u64());
            *maybe_rng = Some(thread_local_rng);
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
            cell.replace(Some(Xoroshiro128Plus::seed_from_u64(seed)));
        });
    }

    #[test]
    fn test_rng() {
        seed_local(42);
        let rand_u64 = with_rng(|rng| rng.next_u64());
        assert_eq!(rand_u64, 16629283624882167704);
    }

    #[test]
    fn test_rng_thread_local() {
        seed_local(42);
        with_rng(|outer_rng| {
            let outer_u64 = outer_rng.next_u64();
            let inner_u64 = std::thread::spawn(move || {
                seed_local(64);
                with_rng(|inner_rng| inner_rng.next_u64())
            })
            .join()
            .unwrap();
            assert_eq!(inner_u64, 18322729244133645280);
            assert_eq!(outer_u64, 16629283624882167704);
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
