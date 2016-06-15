//! The MapReducer trait and associated types.

use record_types::{REmitter, MEmitter, Record, MultiRecord};

use std::clone::Clone;
use std::hash::{Hasher, SipHasher};

/// Default sharding function.
pub fn _std_shard(n: usize, key: &String) -> usize {
    let mut h = SipHasher::new();
    h.write(key.as_bytes());
    h.finish() as usize % n
}

/// Map() function type. The MEmitter argument is used to emit values from
/// the map() function.
pub type MapperF = fn(&mut MEmitter, Record);
/// Reduce() function type. The REmitter argument is used to emit values
/// from the reduce() function.
pub type ReducerF = fn(&mut REmitter, MultiRecord);
/// A function used to determine the shard a key belongs in.
/// The first argument is the number of shards, the second one the key;
/// the return value should be in [0; n).
pub type SharderF = fn(usize, &String) -> usize;

pub trait Mapper: Send + Clone {
    /// Takes one <key,value> pair and an emitter.
    /// The emitter is used to yield results from the map phase.
    ///
    /// Note that this method takes a &mut self; you can use this to cache expensive objects
    /// between runs (but not between shards!)
    fn map(&mut self, em: &mut MEmitter, record: Record);
}

pub trait Reducer: Send + Clone {
    /// Takes one key and one or more values and emits one or more
    /// values.
    ///
    /// Note that this method takes a &mut self; you can use this to cache expensive objects
    /// between runs (but not between shards!)
    fn reduce(&mut self, em: &mut REmitter, records: MultiRecord);
}

pub trait Sharder: Send + Clone {
    /// Determines how to map keys to (reduce) shards.
    /// Returns a number in [0; n) determining the shard the key belongs in.
    /// The default implementation uses a simple hash (SipHasher) and modulo.
    fn shard(&mut self, n: usize, key: &String) -> usize {
        _std_shard(n, key)
    }
}

pub struct DefaultSharder;
