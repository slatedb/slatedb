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
//! ```rust
//! use slate_db::rand::seed;
//!
//! seed(42);
//! let _ = rng().next_u64();
//! ```

use std::cell::RefCell;
use std::sync::{Mutex, OnceLock};

use rand_core::{RngCore, SeedableRng};
use rand_xoshiro::Xoroshiro128PlusPlus;

type RngAlg = Xoroshiro128PlusPlus;

static ROOT_RNG: OnceLock<Mutex<RngAlg>> = OnceLock::new();

/// Seed the root random number generator.
///
/// This function can only be called once. If it is called multiple times, it will panic.
pub fn seed(seed: u64) {
    let rng = RngAlg::seed_from_u64(seed);
    ROOT_RNG
        .set(Mutex::new(rng))
        .expect("rand::seed() can only be called once");
}

fn root_lock() -> &'static Mutex<RngAlg> {
    ROOT_RNG.get_or_init(|| Mutex::new(RngAlg::from_os_rng()))
}

thread_local! {
    static THREAD_RNG: RefCell<RngAlg> = {
        let mut guard = root_lock().lock().expect("root rng poisoned");
        let child_seed = guard.next_u64();
        RefCell::new(RngAlg::seed_from_u64(child_seed))
    };
}

// ThreadRng is a zero-sized type that provides a handle to a thread's RNG. We have to
// do this because the RNG needs to be mutable and we can't return a RefMut from the
// `rng()` function (the lifetime of the RefMut would be the lifetime of the `rng()`
// function).

/// A thread-local random number generator.
pub struct ThreadRng;

impl ThreadRng {
    #[inline(always)]
    fn with<R>(f: impl FnOnce(&mut RngAlg) -> R) -> R {
        THREAD_RNG.with(|cell| {
            let mut borrow = cell.borrow_mut();
            f(&mut *borrow)
        })
    }
}

impl RngCore for ThreadRng {
    #[inline]
    fn next_u32(&mut self) -> u32 {
        ThreadRng::with(|rng| rng.next_u32())
    }

    #[inline]
    fn next_u64(&mut self) -> u64 {
        ThreadRng::with(|rng| rng.next_u64())
    }

    #[inline]
    fn fill_bytes(&mut self, dest: &mut [u8]) {
        ThreadRng::with(|rng| rng.fill_bytes(dest))
    }
}

/// Returns a handle to the thread-local random number generator.
#[inline]
pub fn rng() -> ThreadRng {
    ThreadRng
}

#[cfg(test)]
mod tests {
    use super::*;

    // Force a thread-local RNG to use a specific seed so we can test deterministically.
    fn seed_local(seed: u64) {
        THREAD_RNG.with(|cell| {
            cell.replace(RngAlg::seed_from_u64(seed));
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
