use crate::clock::{DefaultLogicalClock, DefaultSystemClock, LogicalClock, SystemClock};
use rand::{RngCore, SeedableRng};
use rand_xoshiro::Xoroshiro128PlusPlus;
use std::{
    cell::RefCell,
    sync::{Arc, Mutex},
};
use thread_local::ThreadLocal;

type RngAlg = Xoroshiro128PlusPlus;

#[derive(Debug)]
pub(crate) struct DbContext {
    root_rng: Mutex<RngAlg>,
    thread_rng: ThreadLocal<RefCell<RngAlg>>,
    system_clock: Arc<dyn SystemClock>,
    logical_clock: Arc<dyn LogicalClock>,
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
            thread_rng: ThreadLocal::new(),
            system_clock,
            logical_clock,
        }
    }

    #[inline]
    pub fn new_rng(&self) -> impl RngCore + Send + Sync {
        let cell = self.thread_rng.get_or(|| {
            let mut guard = self.root_rng.lock().expect("root rng mutex poisoned");
            RefCell::new(RngAlg::seed_from_u64(guard.next_u64()))
        });
        let mut rng_ref = cell.borrow_mut();
        let out = rng_ref.clone();
        rng_ref.jump();
        out
    }

    pub fn system_clock(&self) -> Arc<dyn SystemClock> {
        self.system_clock.clone()
    }

    pub fn logical_clock(&self) -> Arc<dyn LogicalClock> {
        self.logical_clock.clone()
    }
}
