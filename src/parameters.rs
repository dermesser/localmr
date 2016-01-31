//! Parameters for a mapreduce process.
//!

#[derive(Clone)]
pub struct MRParameters {
    pub key_buffer_size: usize,

    pub mappers: usize,
    pub reducers: usize,
    
    pub map_partition_size: usize,

    // Internal parameters
    pub shard_id: usize,
}

impl MRParameters {
    pub fn new() -> MRParameters {
        MRParameters {
            key_buffer_size: 256,
            mappers: 4,
            reducers: 4,
            map_partition_size: 100 * 1024 * 1024,
            shard_id: 0,
        }
    }

    /// An implementation detail: When processing the data during the map phase, this
    /// parameter determines how many keys are processed in direct sequence. Heavily increasing
    /// this value increases memory usage.
    pub fn set_key_buffer_size(mut self, n: usize) -> MRParameters {
        self.key_buffer_size = n;
        self
    }

    /// Determines how many parallel processes will be run. Mappers and reducers do in general
    /// not run at the same time (as the reducers need to wait for the map output). The number of
    /// reducers also determines the sharding of the map output data.
    pub fn set_concurrency(mut self, mappers: usize, reducers: usize) -> MRParameters {
        self.mappers = mappers;
        self.reducers = reducers;
        self
    }

    /// This parameter determines the size of the chunks that the input is partitioned in
    /// before being processed by map shards. More memory usually also means faster processing;
    /// however, entire chunks are held in memory at once, so your available RAM is the limit.
    /// In general: All input data of one chunk will be in memory; all output data will be in
    /// memory, too; but both are not in memory at the full size at the same time (as input data
    /// are consumed the output data builds up, and the memory taken up by the former is released).
    pub fn set_partition_size(mut self, size: usize) -> MRParameters {
        self.map_partition_size = size;
        self
    }

    /// For internal use: Sets the ID of the executing data chunk (for file naming etc.)
    pub fn set_shard_id(mut self, n: usize) -> MRParameters {
        self.shard_id = n;
        self
    }
}
