//! SlateDB's module for generating random data.
//!
//! This module exists because we want to do deterministic simulation testing for SlateDB.
//! To do so, we need a way to easily seed all random number generators in the library in a
//! deterministic way.
//!
//! Each thread also has a thread-local random number generator that is initialized from a
//! root-level random number generator. This ensures that each thread is seeded
//! deterministically from the root RNG.
//!
//! The root-level random number generator starts unset. It is set in one of two ways:
//!
//! - `seed(seed: u64)` is called with a seed value.
//! - `thread_rng()` is called while the root RNG is unset. This will initialize the root RNG
//!   with a random seed.
//!
//! ## Usage
//!
//! ```ignore
//! seed(42);
//! let _ = rng().next_u64();
//! ```

use std::cell::UnsafeCell;
use std::sync::{Mutex, OnceLock};

use rand_core::{RngCore, SeedableRng};
use rand_xoshiro::Xoroshiro128PlusPlus;

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
    static THREAD_RNG: UnsafeCell<RngAlg> = {
        let mut guard = ROOT_RNG
            .get_or_init(|| Mutex::new(RngAlg::from_os_rng()))
            .lock()
            .expect("root rng poisoned");
        let child_seed = guard.next_u64();
        UnsafeCell::new(RngAlg::seed_from_u64(child_seed))
    };
}

// ThreadRng is a zero-sized type that provides a handle to a thread's RNG. We have to
// do this because the RNG needs to be mutable and we can't return a RefMut from the
// `rng()` function (the lifetime of the RefMut would be the lifetime of the `rng()`
// function). We are using `UnsafeCell` because the THREAD_RNG is thread-local by
// design. See the `rand` crate for more details:
// https://github.com/rust-random/rand/blob/204084a35fc7289e9a38575fdd80869818484517/src/rngs/thread.rs#L24-L34

/// A thread-local random number generator.
pub(crate) struct ThreadRng;

impl RngCore for ThreadRng {
    #[inline(always)]
    fn next_u32(&mut self) -> u32 {
        // SAFETY: We must make sure to stop using `rng` before anyone else
        // creates another mutable reference
        let rng = unsafe { &mut *THREAD_RNG.with(|cell| cell.get()) };
        rng.next_u32()
    }

    #[inline(always)]
    fn next_u64(&mut self) -> u64 {
        // SAFETY: We must make sure to stop using `rng` before anyone else
        // creates another mutable reference
        let rng = unsafe { &mut *THREAD_RNG.with(|cell| cell.get()) };
        rng.next_u64()
    }

    #[inline(always)]
    fn fill_bytes(&mut self, dest: &mut [u8]) {
        // SAFETY: We must make sure to stop using `rng` before anyone else
        // creates another mutable reference
        let rng = unsafe { &mut *THREAD_RNG.with(|cell| cell.get()) };
        rng.fill_bytes(dest)
    }
}

/// Returns a handle to the thread-local random number generator.
#[inline]
pub(crate) fn rng() -> ThreadRng {
    ThreadRng
}

#[cfg(test)]
mod tests {
    use super::*;

    // Force a thread-local RNG to use a specific seed so we can test deterministically.
    fn seed_local(seed: u64) {
        THREAD_RNG.with(|cell| {
            let rng = unsafe { &mut *cell.get() };
            *rng = RngAlg::seed_from_u64(seed);
        });
    }

    #[test]
    fn test_rng() {
        std::thread::spawn(move || {
            seed_local(42);
            let rand_u64 = rng().next_u64();
            assert_eq!(rand_u64, 16756476715040848931);
        })
        .join()
        .unwrap();
    }

    #[test]
    fn test_rng_thread_local() {
        std::thread::spawn(move || {
            seed_local(42);
            let outer_u64 = rng().next_u64();
            let inner_u64 = std::thread::spawn(move || {
                seed_local(64);
                rng().next_u64()
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
