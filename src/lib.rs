#![warn(clippy::unwrap_used)]
#![cfg_attr(test, allow(clippy::unwrap_used))]
#![warn(clippy::panic)]
#![cfg_attr(test, allow(clippy::panic))]

mod block;
mod block_iterator;
pub mod db;
mod error;
mod flush;
mod iter;
mod mem_table;
mod sst;
mod tablestore;
