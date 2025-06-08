use rand::{Rng as _, RngCore, SeedableRng};
use rand_xoshiro::Xoroshiro128PlusPlus;
use thread_local::ThreadLocal;
use ulid::Ulid;
use uuid::Uuid;

use crate::clock::{DefaultLogicalClock, DefaultSystemClock, LogicalClock, SystemClock};
use std::sync::{Arc, Mutex};

type RngAlg = Xoroshiro128PlusPlus;

#[derive(Debug)]
pub(crate) struct DbContext {
    root_rng: Mutex<RngAlg>,
    thread_rng: ThreadLocal<Box<ThreadRng>>,
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

    pub fn thread_rng(&self) -> &ThreadRng {
        self.thread_rng.get_or(|| {
            let mut guard = self.root_rng.lock().expect("root rng poisoned");
            let child_seed = guard.next_u64();
            Box::new(ThreadRng { rng: Box::new(RngAlg::seed_from_u64(child_seed)) })
        })
    }

    pub fn system_clock(&self) -> Arc<dyn SystemClock> {
        self.system_clock.clone()
    }

    pub fn logical_clock(&self) -> Arc<dyn LogicalClock> {
        self.logical_clock.clone()
    }
}

pub(crate) struct ThreadRng {
    rng: Box<RngAlg>
}

impl std::fmt::Debug for ThreadRng {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ThreadRng").finish()
    }
}

impl RngCore for ThreadRng {
    #[inline(always)]
    fn next_u32(&mut self) -> u32 {
        self.rng.next_u32()
    }

    #[inline(always)]
    fn next_u64(&mut self) -> u64 {
        self.rng.next_u64()
    }

    #[inline(always)]
    fn fill_bytes(&mut self, dest: &mut [u8]) {
        self.rng.fill_bytes(dest)
    }

    #[inline(always)]
    fn try_fill_bytes(&mut self, dest: &mut [u8]) -> Result<(), rand::Error> {
        self.rng.try_fill_bytes(dest)
    }
}

impl ThreadRng {
    #[inline(always)]
    fn uuid(&mut self) -> Uuid {
        let rng = self.rng.as_mut();
        Uuid::from_bytes(rng.gen())
    }

    #[inline(always)]
    fn ulid(&mut self) -> Ulid {
        let rng = self.rng.as_mut();
        Ulid::from_bytes(rng.gen())
    }
}