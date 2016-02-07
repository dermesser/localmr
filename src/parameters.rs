//! Parameters for a mapreduce process.
//!

#[derive(Clone)]
pub struct MRParameters {
    pub key_buffer_size: usize,

    pub mappers: usize,
    pub reducers: usize,

    pub map_partition_size: usize,

    pub reduce_group_prealloc_size: usize,
    pub reduce_group_insensitive: bool,

    pub map_output_location: String,
    pub keep_temp_files: bool,
    pub reduce_output_shard_prefix: String,

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
            reduce_group_prealloc_size: 1,
            reduce_group_insensitive: false,
            map_output_location: String::from("map_intermediate_"),
            keep_temp_files: false,
            reduce_output_shard_prefix: String::from("output_"),
            shard_id: 0,
        }
    }

    /// An implementation detail: When processing the data during the map phase, this
    /// parameter determines how many keys are processed in direct sequence. Heavily increasing
    /// this value increases memory usage.
    ///
    /// Default 256
    pub fn set_key_buffer_size(mut self, n: usize) -> MRParameters {
        self.key_buffer_size = n;
        self
    }

    /// Determines how many parallel processes will be run. Mappers and reducers do in general
    /// not run at the same time (as the reducers need to wait for the map output). The number of
    /// reducers also determines the sharding of the map output data.
    ///
    /// Default 4/4
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
    ///
    /// Default 100 MiB
    pub fn set_partition_size(mut self, size: usize) -> MRParameters {
        self.map_partition_size = size;
        self
    }

    /// prealloc_size: How big are the groups of keys in the reduce phase expected to be? (used for pre-allocating
    /// buffers)
    /// Default 1.
    ///
    /// insensitive: Whether to group strings together that differ in case. When used, the first
    /// encountered key will be supplied as key to the reduce function.
    /// BUG: This will not work correctly until the map phase delivers outputs in the correct order, i.e.
    /// dictionary order. The default Ord implementation for String treats lower and upper case
    /// very differently. Default: false.
    pub fn set_reduce_group_opts(mut self,
                                 prealloc_size: usize,
                                 insensitive: bool)
                                 -> MRParameters {
        self.reduce_group_prealloc_size = prealloc_size;
        self.reduce_group_insensitive = insensitive;
        self
    }

    /// map_out_prefix: A location that can be used for intermediate map outputs. For example,
    /// '/home/user/processing/tmp/'. (Note: Make sure that the location provides enough disk
    /// space). Default: './output_' (will lead to ./output_0, ./output_1 etc.)
    ///
    /// reduce_out_prefix: Path prefix for output files produced by the reduce phase, for example
    /// '/home/user/processing/output_'. (Note: Make sure that the location provides enough
    /// disk space). Default: './map_intermediate_' (will lead to ./map_intermediate_0.0 etc.)
    ///
    pub fn set_file_locations(mut self,
                              map_out_prefix: String,
                              reduce_out_prefix: String)
                              -> MRParameters {
        self.map_output_location = map_out_prefix;
        self.reduce_output_shard_prefix = reduce_out_prefix;
        self
    }

    /// If this is set to true, intermediate files, such as outputs from the map phase,
    /// will be kept.
    ///
    /// Default: false
    pub fn keep_temp_files(mut self, keep: bool) -> MRParameters {
        self.keep_temp_files = keep;
        self
    }

    /// For internal use: Sets the ID of the executing data chunk (for file naming etc.)
    ///
    pub fn set_shard_id(mut self, n: usize) -> MRParameters {
        self.shard_id = n;
        self
    }
}
