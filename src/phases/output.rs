use std::io;
use formats::util::RecordReadIterator;
use formats::writelog::WriteLogReader;
use parameters::MRParameters;

fn map_output_name(base: &String, mapper: usize, shard: usize) -> String {
    format!("{}-{}.{}", base, mapper, shard)
}

/// A type implementing SinkGenerator is used at the end of the reducer
/// phase to write the output. Given a name, new() should return a new object
/// that can be used to write the output of a reduce partition.
/// Values are always written as a whole to the writer.
///
/// SinkGenerator types are used in general to determine the format of outputs; existing options
/// are plain text files (LinesSinkGenerator) or length-prefixed binary files (WriteLogGenerator).
pub trait SinkGenerator: Send + Clone {
    type Sink: io::Write;
    /// Return a new intermediary file handle destined for reduce shard `shard` and requested by
    /// map shard `mapper`.
    fn new_map_output(&self, location: &String, mapper: usize, shard: usize) -> Self::Sink {
        self.new_output(&map_output_name(location, mapper, shard))
    }

    /// Return a new file handle for `location`.
    fn new_output(&self, location: &String) -> Self::Sink;
}

pub fn open_reduce_inputs(location: &String,
                      partitions: usize,
                      shard: usize)
                      -> Vec<RecordReadIterator<WriteLogReader>> {
    let mut inputs = Vec::new();

    for part in 0..partitions {
        let name = map_output_name(location, part, shard);
        let wlg_reader = WriteLogReader::new_from_file(&name).unwrap();
        inputs.push(RecordReadIterator::new(wlg_reader));
    }
    inputs
}

/// Calculates the name of a reduce output shard from the parameters.
pub fn get_reduce_output_name(params: &MRParameters) -> String {
    format!("{}{}", params.reduce_output_shard_prefix, params.shard_id)
}
