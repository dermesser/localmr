use mapreducer::{MEmitter, MapReducer, MapperF, MultiRecord, REmitter, Record, ReducerF};

struct ClosureMapReducer {
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
