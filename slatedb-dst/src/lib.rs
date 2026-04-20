//! Deterministic scenario testing utilities and fault-injecting object stores
//! for SlateDB.
//!
//! The crate re-exports the harness API and the public fault-injection types
//! used to build deterministic DST scenarios.

#![doc = include_str!("../README.md")]
#![cfg(tokio_unstable)]

mod clocked_object_store;
pub mod failing_object_store;
mod harness;

pub use self::failing_object_store::{
    FailingObjectStore, FailingObjectStoreController, HttpFailBefore, HttpStatusError, Operation,
    StreamDirection, Toxic, ToxicKind,
};
pub use self::harness::*;
