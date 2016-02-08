//! Implements a mapreduce process bounded to one machine;
//! this is supposed to result in better data parallelization.
//!

pub mod closure_mr;
pub mod controller;
pub mod formats;
pub mod input_cache;
mod map;
pub mod mapreducer;
pub mod parameters;
pub mod record_types;
mod reduce;
mod shard_merge;
mod sort;

#[test]
fn it_works() {}
