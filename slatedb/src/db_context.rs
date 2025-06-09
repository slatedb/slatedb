#![allow(clippy::disallowed_types, clippy::disallowed_methods)]
use crate::clock::{DefaultLogicalClock, DefaultSystemClock, LogicalClock, SystemClock};
use rand::{RngCore, SeedableRng};
use rand_xoshiro::Xoroshiro128PlusPlus;
use std::{
    cell::RefCell,
    ops::DerefMut,
    sync::{Arc, Mutex},
};
use thread_local::ThreadLocal;

type RngAlg = Xoroshiro128PlusPlus;

#[derive(Debug)]
pub struct DbContext {
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

    pub fn thread_rng(&self) -> ThreadRng {
        ThreadRng::new(self)
    }

    pub fn system_clock(&self) -> Arc<dyn SystemClock> {
        self.system_clock.clone()
    }

    pub fn logical_clock(&self) -> Arc<dyn LogicalClock> {
        self.logical_clock.clone()
    }

    #[inline]
    fn with_rng<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&mut RngAlg) -> R,
    {
        let cell = self.thread_rng.get_or(|| {
            let mut guard = self.root_rng.lock().expect("root rng mutex poisoned");
            guard.jump();
            RefCell::new(RngAlg::seed_from_u64(guard.next_u64()))
        });
        let mut rng_ref = cell.borrow_mut();
        f(rng_ref.deref_mut())
    }
}

pub struct ThreadRng<'a> {
    context: &'a DbContext,
}

impl<'a> ThreadRng<'a> {
    pub fn new(context: &'a DbContext) -> Self {
        Self { context }
    }
}

impl<'a> RngCore for ThreadRng<'a> {
    fn next_u32(&mut self) -> u32 {
        self.context.with_rng(|rng| rng.next_u32())
    }

    fn next_u64(&mut self) -> u64 {
        self.context.with_rng(|rng| rng.next_u64())
    }

    fn fill_bytes(&mut self, dest: &mut [u8]) {
        self.context.with_rng(|rng| rng.fill_bytes(dest))
    }

    fn try_fill_bytes(&mut self, dest: &mut [u8]) -> Result<(), rand::Error> {
        self.context.with_rng(|rng| rng.try_fill_bytes(dest))
    }
}
