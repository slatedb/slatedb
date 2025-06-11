#![allow(clippy::disallowed_types)]

//! SlateDB's module for generating random data.
//!
//! This module exists because we want to do deterministic simulation testing
//! for SlateDB. To do so, we need a way to easily seed all random number
//! generators (RNGs) in the library in a deterministic way.
//!
//! The module currently uses Xoshiro128++ for all random number generators. The
//! rand crate's `seed_from_u64` method uses SplitMix64 under the hood, which
//! makes it safe to use when creating new thread-local RNGs.

use std::cell::RefCell;
use std::sync::atomic::{AtomicU64, Ordering};

use rand::RngCore;
use rand::SeedableRng;
use rand_xoshiro::Xoroshiro128PlusPlus;
use thread_local::ThreadLocal;

type RngAlg = Xoroshiro128PlusPlus;

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
pub struct DbRand {
    seed_counter: AtomicU64,
    thread_rng: ThreadLocal<RefCell<RngAlg>>,
}

impl DbRand {
    /// Create a new `DbRand` with the given 64-bit seed.
    pub fn new(seed: u64) -> Self {
        DbRand {
            seed_counter: AtomicU64::new(seed),
            thread_rng: ThreadLocal::new(),
        }
    }

    /// Grab this thread’s RNG. Initializes the RNG if it hasn't been initialized yet.
    pub fn thread_rng(&self) -> std::cell::RefMut<'_, impl RngCore> {
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
