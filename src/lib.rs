//! Implements a mapreduce process bounded to one machine;
//! this is supposed to result in better data parallelization.
//!

pub mod closure_mr;
pub mod formats;
pub mod map;
pub mod mapreducer;
pub mod parameters;


#[test]
fn it_works() {}
