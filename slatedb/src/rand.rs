//! SlateDB's module for generating random data.
//!
//! This module exists because we want to do deterministic simulation testing
//! for SlateDB. To do so, we need a way to easily seed all random number
//! generators (RNGs) in the library in a deterministic way.
//!
//! The module currently uses Xoshiro128++ for all random number generators. The
//! rand crate's `seed_from_u64` method uses SplitMix64 under the hood, which
//! makes it safe to use when creating new thread-local RNGs.

#![allow(clippy::disallowed_types, clippy::disallowed_methods)]

use std::cell::RefCell;
use std::sync::atomic::{AtomicU64, Ordering};

use rand::RngCore;
use rand::SeedableRng;
use rand_xoshiro::Xoroshiro128PlusPlus;
use thread_local::ThreadLocal;

pub(crate) type RngAlg = Xoroshiro128PlusPlus;

/// A shareable, lock-free random number generator (RNG).
///
/// This is a wrapper around `ThreadLocal<RngAlg>` that provides a thread-local
/// random number generator for each thread. If a custom seed is provided,
/// each thread will be seeded deterministically from the seed, and the seed will
/// be incremented for the next thread.
///
/// Note that if there is more than one thread, the RNG's behavior is still
/// non-deterministic since it depends on which thread is scheduled first by the
/// OS scheduler. Only one thread must be used for the entire SlateDB runtime if
/// deterministic behavior is desired.
///
/// ## Usage
///
/// ```ignore
/// use slate_db::rand::DbRand;
///
/// let rng = DbRand::new(42);
/// let _ = rng.next_u64();
/// ```
#[derive(Debug)]
pub(crate) struct DbRand {
    seed_counter: AtomicU64,
    thread_rng: ThreadLocal<RefCell<RngAlg>>,
}

impl DbRand {
    /// Create a new `DbRand` with the given 64-bit seed.
    pub(crate) fn new(seed: u64) -> Self {
        DbRand {
            seed_counter: AtomicU64::new(seed),
            thread_rng: ThreadLocal::new(),
        }
    }

    /// Grab this thread’s RNG. Initializes the RNG if it hasn't been initialized yet.
    pub(crate) fn rng(&self) -> std::cell::RefMut<'_, impl RngCore> {
        self.thread_rng
            .get_or(|| {
                let seed = self.seed_counter.fetch_add(1, Ordering::Relaxed);
                // seed_from_u64 inlines SplitMix64, which whitens the incremental seed
                RefCell::new(RngAlg::seed_from_u64(seed))
            })
            .borrow_mut()
    }
}

impl Default for DbRand {
    fn default() -> Self {
        Self::new(rand::random())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::borrow::BorrowMut;
    use std::collections::HashSet;
    use std::sync::Arc;
    use std::thread;

    #[test]
    fn test_deterministic_behavior() {
        // Same seed should produce same sequence of random numbers
        let rng1 = DbRand::new(42);
        let rng2 = DbRand::new(42);

        let mut values1 = Vec::new();
        let mut values2 = Vec::new();

        // Generate some random numbers
        for _ in 0..10 {
            values1.push(rng1.rng().next_u64());
            values2.push(rng2.rng().next_u64());
        }

        println!("values1: {:?}", values1);
        println!("values2: {:?}", values2);

        assert_eq!(values1, values2);
    }

    #[test]
    fn test_different_seeds_produce_different_sequences() {
        let rng1 = DbRand::new(42);
        let rng2 = DbRand::new(43);

        let mut values1 = Vec::new();
        let mut values2 = Vec::new();

        // Generate some random numbers
        for _ in 0..10 {
            values1.push(rng1.rng().next_u64());
            values2.push(rng2.rng().next_u64());
        }

        println!("values1: {:?}", values1);
        println!("values2: {:?}", values2);

        // Very small chance this would randomly fail, but practically zero
        assert_ne!(values1, values2);
    }

    #[test]
    fn test_thread_rngs_differ_across_threads() {
        // Create a DbRand instance with a known seed
        let rand = Arc::new(DbRand::new(100));

        // Number of threads to spawn
        let n_threads = 4;

        // Collect first random number from each thread
        let thread_handles: Vec<_> = (0..n_threads)
            .map(|_| {
                let rand = Arc::clone(&rand);

                thread::spawn(move || {
                    // Get the pointer to this thread's RNG and return it as a
                    // usize. This is used to verify that each thread has a
                    // different RNG.
                    let ptr = &mut *(rand.rng().borrow_mut()) as *mut _;
                    ptr as usize
                })
            })
            .collect();

        // Collect results from all threads
        let results: Vec<usize> = thread_handles
            .into_iter()
            .map(|handle| handle.join().unwrap())
            .collect();

        // Create a HashSet to check for duplicates
        let unique_values: HashSet<_> = results.iter().cloned().collect();

        // Each thread should get a different RNG with a different seed,
        // so we should have n_threads unique values
        assert_eq!(
            unique_values.len(),
            n_threads,
            "Expected {} unique random values from threads, got {}. \
                   This indicates thread RNGs are not different across threads.",
            n_threads,
            unique_values.len()
        );
    }
}
