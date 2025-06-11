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
use std::sync::{Mutex, OnceLock};

use rand::{RngCore, SeedableRng};
use rand_xoshiro::Xoroshiro128PlusPlus;
use thread_local::ThreadLocal;

type RngAlg = Xoroshiro128PlusPlus;

static ROOT_RNG: OnceLock<Mutex<RngAlg>> = OnceLock::new();

/// Seed the root random number generator.
///
/// This function can only be called once. If it is called multiple times, it will panic.
pub(crate) fn seed(seed: u64) {
    let rng = RngAlg::seed_from_u64(seed);
    ROOT_RNG
        .set(Mutex::new(rng))
        .expect("rand::seed() can only be called once");
}

thread_local! {
    /// Per-thread RNG state, stored by value in a Cell.
    static THREAD_RNG: RefCell<RngAlg> = {
        let mut guard = ROOT_RNG
            .get_or_init(|| Mutex::new(RngAlg::from_entropy()))
            .lock()
            .expect("root rng poisoned");
        let child_seed = guard.next_u64();
        RefCell::new(RngAlg::seed_from_u64(child_seed))
    };
}

/// A thread-local random number generator.
pub(crate) struct ThreadRng;

impl RngCore for ThreadRng {
    #[inline(always)]
    fn next_u32(&mut self) -> u32 {
        THREAD_RNG.with(|cell| cell.borrow_mut().next_u32())
    }

    #[inline(always)]
    fn next_u64(&mut self) -> u64 {
        THREAD_RNG.with(|cell| cell.borrow_mut().next_u64())
    }

    #[inline(always)]
    fn fill_bytes(&mut self, dest: &mut [u8]) {
        THREAD_RNG.with(|cell| cell.borrow_mut().fill_bytes(dest))
    }

    #[inline(always)]
    fn try_fill_bytes(&mut self, dest: &mut [u8]) -> Result<(), rand::Error> {
        THREAD_RNG.with(|cell| cell.borrow_mut().try_fill_bytes(dest))
    }
}

/// Returns a handle to the thread-local random number generator.
#[inline]
pub(crate) fn thread_rng() -> ThreadRng {
    ThreadRng
}

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
/// use slate_db::rand::DbRng;
///
/// let rng = DbRng::new(42);
/// let _ = rng.next_u64();
/// ```
pub struct DbRng {
    seed_counter: AtomicU64,
    thread_rng: ThreadLocal<RefCell<RngAlg>>,
}

impl DbRng {
    /// Create a new `DbRng` with the given 64-bit seed.
    pub fn new(seed: u64) -> Self {
        DbRng {
            seed_counter: AtomicU64::new(seed),
            thread_rng: ThreadLocal::new(),
        }
    }

    /// Grab (or initialize) this thread’s RNG
    fn thread_rng(&self) -> std::cell::RefMut<'_, RngAlg> {
        self.thread_rng
            .get_or(|| {
                let seed = self.seed_counter.fetch_add(1, Ordering::Relaxed);
                // seed_from_u64 inlines SplitMix64, which whitens the incremental seed
                RefCell::new(RngAlg::seed_from_u64(seed))
            })
            .borrow_mut()
    }
}

impl RngCore for DbRng {
    /// Generate a random u64
    fn next_u64(&mut self) -> u64 {
        self.thread_rng().next_u64()
    }

    /// Generate a random u32
    fn next_u32(&mut self) -> u32 {
        self.thread_rng().next_u32()
    }

    /// Fill bytes
    fn fill_bytes(&mut self, dest: &mut [u8]) {
        self.thread_rng().fill_bytes(dest)
    }

    /// Try to fill bytes
    fn try_fill_bytes(&mut self, dest: &mut [u8]) -> Result<(), rand::Error> {
        self.thread_rng().try_fill_bytes(dest)
    }
}

impl Default for DbRng {
    fn default() -> Self {
        #![allow(clippy::disallowed_methods)]
        Self::new(rand::random())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Force a thread-local RNG to use a specific seed so we can test deterministically.
    fn seed_local(seed: u64) {
        THREAD_RNG.with(|cell| {
            *cell.borrow_mut() = RngAlg::seed_from_u64(seed);
        });
    }

    #[test]
    fn test_rng() {
        std::thread::spawn(move || {
            seed_local(42);
            let rand_u64 = thread_rng().next_u64();
            assert_eq!(rand_u64, 16756476715040848931);
        })
        .join()
        .unwrap();
    }

    #[test]
    fn test_rng_thread_local() {
        std::thread::spawn(move || {
            seed_local(42);
            let outer_u64 = thread_rng().next_u64();
            let inner_u64 = std::thread::spawn(move || {
                seed_local(64);
                thread_rng().next_u64()
            })
            .join()
            .unwrap();
            assert_eq!(inner_u64, 12172458793332410705);
            assert_eq!(outer_u64, 16756476715040848931);
        })
        .join()
        .unwrap();
    }
}
