//! Implements the Reduce phase.
//!

use mapreducer::MapReducer;
use parameters::MRParameters;
use formats::util::MRSinkGenerator;
use record_types::{Record, MultiRecord, REmitter};
use shard_merge::ShardMergeIterator;

struct ReducePartition<MR: MapReducer, InputIt: Iterator<Item = Record>, SinkGen: MRSinkGenerator> {
    mr: MR,
    params: MRParameters,
    // Maybe we want to genericize this to an Iterator<Item=Read> or so? This defers opening
    // the files to the reduce shard itself.
    srcfiles: Vec<InputIt>,
    dstfilegen: SinkGen,
}

impl<MR: MapReducer, InputIt: Iterator<Item=Record>, SinkGen: MRSinkGenerator> ReducePartition<MR, InputIt, SinkGen> {
/// Create a new Reduce partition for the given MR; source and destination I/O.
/// mr is the map/reduce functions.
/// params is generic MR parameters as well as some applying directly to this reduce partition.
/// srcfiles is a set of Iterator<Item=Record>s. Those are usually reading from the map phase's
/// outputs.
/// dstfiles is a SinkGen (as known from the mapping phase) that is used to create the output
/// file (there is one output file per reduce partition, currently).
    pub fn new(mr: MR, params: MRParameters, srcfiles: Vec<InputIt>, dstfiles: SinkGen) -> ReducePartition<MR, InputIt, SinkGen> {
        ReducePartition { mr: mr, params: params, srcfiles: srcfiles, dstfilegen: dstfiles }
    }

/// Run the Reduce partition.
    pub fn _run(mut self) {
        let mut sorted_input = self.open_sorted_input();
// reduce input and write results.
    }

/// Create an iterator that merges all input sources. Leaves self.srcfiles empty.
    fn open_sorted_input(&mut self) -> ShardMergeIterator<Record> {
        let mut inputs = Vec::new();
        inputs.append(&mut self.srcfiles);
        let mut it = inputs.into_iter();

        ShardMergeIterator::build(&mut it)
    }

}

use std::iter::Peekable;

/// Iterator adapter: Converts an Iterator<Item=Record> into an Iterator<Item=MultiRecord> by
/// grouping subsequent records with identical key.
/// The original iterator must yield records in sorted order (or at least in an order where
/// identical items are adjacent).
struct RecordsToMultiRecords<It: Iterator<Item = Record>> {
    it: Peekable<It>,
    /// Efficiency knob: How big groups of records are expected to be. Default is 1.
    expected_group_size: usize,
}

impl<It: Iterator<Item = Record>> RecordsToMultiRecords<It> {
    fn new(it: It, egs: usize) -> RecordsToMultiRecords<It> {
        RecordsToMultiRecords {
            it: it.peekable(),
            expected_group_size: egs,
        }
    }
}

impl<It: Iterator<Item = Record>> Iterator for RecordsToMultiRecords<It> {
    type Item = MultiRecord;
    fn next(&mut self) -> Option<Self::Item> {
        let mut collection = Vec::with_capacity(self.expected_group_size);
        let key: String;

        match self.it.next() {
            None => return None,
            Some(r) => {
                key = r.key;
                collection.push(r.value)
            }
        }

        loop {
            match self.it.peek() {
                None => break,
                Some(r) => {
                    if r.key != key {
                        break;
                    }
                }
            }
            collection.push(self.it.next().unwrap().value);
        }

        return Some(MultiRecord::new(key, collection));
    }
}
