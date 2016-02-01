//! A MapReducer that uses supplied map()/reduce() functions.

use mapreducer::{MapReducer, MapperF, ReducerF, SharderF, _std_shard};
use record_types::{Record, MultiRecord, MEmitter, REmitter};

/// This type implements the MapReducer trait. You can use it to provide your own functions to a
/// MapReduce process. If you need more flexibility, however, you may want to simply implement your
/// own type that fulfills MapReducer.
pub struct ClosureMapReducer {
    mapper: MapperF,
    reducer: ReducerF,
    sharder: SharderF,
}

impl Clone for ClosureMapReducer {
    fn clone(&self) -> ClosureMapReducer {
        ClosureMapReducer {
            mapper: self.mapper,
            reducer: self.reducer,
            sharder: self.sharder,
        }
    }
}

impl ClosureMapReducer {
    /// Create a new MapReducer from the supplied functions.
    pub fn new(mapper: MapperF, reducer: ReducerF) -> ClosureMapReducer {
        ClosureMapReducer {
            mapper: mapper,
            reducer: reducer,
            sharder: _std_shard,
        }
    }
    /// Set the function used for sharding.
    pub fn set_sharder(&mut self, s: SharderF) {
        self.sharder = s;
    }
}

impl MapReducer for ClosureMapReducer {
    fn map(&self, e: &mut MEmitter, r: Record) {
        (self.mapper)(e, r)
    }
    fn reduce(&self, e: &mut REmitter, r: MultiRecord) {
        (self.reducer)(e, r)
    }
    fn shard(&self, n: usize, k: &String) -> usize {
        (self.sharder)(n, k)
    }
}
