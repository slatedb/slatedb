#![doc = include_str!("../README.md")]

mod dst;
pub mod utils;

#[allow(unused_imports)]
pub use dst::{
    DefaultDstDistribution, Dst, DstAction, DstDistribution, DstDuration, DstOptions, DstWriteOp,
};
