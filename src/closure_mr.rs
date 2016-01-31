//! A MapReducer that uses supplied map()/reduce() functions.

use mapreducer::{MEmitter, MapReducer, MapperF, MultiRecord, REmitter, Record, ReducerF};

/// Use your functions in a MapReduce (instead of implementing your own mapreducer)
pub struct ClosureMapReducer {
    mapper: MapperF,
    reducer: ReducerF,
}

impl Clone for ClosureMapReducer {
    fn clone(&self) -> ClosureMapReducer {
        ClosureMapReducer {
            mapper: self.mapper,
            reducer: self.reducer,
        }
    }
}

impl ClosureMapReducer {
    /// Create a new MapReducer from the supplied functions.
    pub fn new(mapper: MapperF, reducer: ReducerF) -> ClosureMapReducer {
        ClosureMapReducer {
            mapper: mapper,
            reducer: reducer,
        }
    }
}

impl MapReducer for ClosureMapReducer {
    fn map(&self, e: &mut MEmitter, r: Record) {
        (self.mapper)(e, r)
    }
    fn reduce(&self, e: &mut REmitter, r: MultiRecord) {
        (self.reducer)(e, r)
    }
}
