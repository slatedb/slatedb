use once_cell::unsync::OnceCell;
use rand::{RngCore, SeedableRng};
use rand_xoshiro::Xoroshiro128PlusPlus;

use crate::clock::{DefaultLogicalClock, DefaultSystemClock, LogicalClock, SystemClock};
use std::sync::{Arc, Mutex};

type RngAlg = Xoroshiro128PlusPlus;

#[derive(Debug)]
pub(crate) struct DbContext {
    root_rng: Mutex<RngAlg>,
    system_clock: Arc<dyn SystemClock>,
    logical_clock: Arc<dyn LogicalClock>,
}

thread_local! {
    /// Per-thread RNG state, stored by value in a Cell.
    static THREAD_RNG: OnceCell<Arc<RngAlg>> = OnceCell::new();
}

impl Default for DbContext {
    fn default() -> Self {
        Self::new(
            rand::thread_rng().next_u64(),
            Arc::new(DefaultSystemClock::default()),
            Arc::new(DefaultLogicalClock::default()),
        )
    }
}

impl DbContext {
    pub fn new(
        rng_seed: u64,
        system_clock: Arc<dyn SystemClock>,
        logical_clock: Arc<dyn LogicalClock>,
    ) -> Self {
        Self {
            root_rng: Mutex::new(RngAlg::seed_from_u64(rng_seed)),
            system_clock,
            logical_clock,
        }
    }

    pub fn rand(&self) -> Arc<dyn RngCore> {
        THREAD_RNG.with(|cell| {
            cell.get_or_init(|| {
                let mut guard = self.root_rng.lock().unwrap();
                let child_seed = guard.next_u64();
                Arc::new(RngAlg::seed_from_u64(child_seed))
            })
            .clone()
        })
    }

    pub fn system_clock(&self) -> Arc<dyn SystemClock> {
        self.system_clock.clone()
    }

    pub fn logical_clock(&self) -> Arc<dyn LogicalClock> {
        self.logical_clock.clone()
    }
}
