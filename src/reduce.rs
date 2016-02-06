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
        let mut inputs = Vec::new();
        inputs.append(&mut self.srcfiles);
        let mut it = inputs.into_iter();

        let params = self.params.clone();

        self.reduce(RecordsToMultiRecords::new(ShardMergeIterator::build(&mut it), params))
    }

    fn get_output_name(&self) -> String {
        use std::fmt;
        let mut name = String::new();
        name.push_str(&self.params.reduce_output_shard_prefix[..]);
        name.push_str(&fmt::format(format_args!("{}", self.params.shard_id))[..]);
        name
    }

    fn reduce<RecIt: Iterator<Item=Record>>(mut self, inp: RecordsToMultiRecords<RecIt>) {
        use std::io::Write;

        let name = self.get_output_name();
        let mut outp = self.dstfilegen.new_output(&name);

        for multirec in inp {
            let mut emitter = REmitter::new();
            self.mr.reduce(&mut emitter, multirec);

            for result in emitter._get().into_iter() {
                outp.write(result.as_bytes());
            }
        }
    }
}

use std::iter::Peekable;

/// Iterator adapter: Converts an Iterator<Item=Record> into an Iterator<Item=MultiRecord> by
/// grouping subsequent records with identical key.
/// The original iterator must yield records in sorted order (or at least in an order where
/// identical items are adjacent).
pub struct RecordsToMultiRecords<It: Iterator<Item = Record>> {
    it: Peekable<It>,
    settings: MRParameters,
}

impl<It: Iterator<Item = Record>> RecordsToMultiRecords<It> {
    fn new(it: It, settings: MRParameters) -> RecordsToMultiRecords<It> {
        RecordsToMultiRecords {
            it: it.peekable(),
            settings: settings,
        }
    }
}

impl<It: Iterator<Item = Record>> Iterator for RecordsToMultiRecords<It> {
    type Item = MultiRecord;
    fn next(&mut self) -> Option<Self::Item> {
        use std::ascii::AsciiExt;
        let mut collection = Vec::with_capacity(self.settings.reduce_group_prealloc_size);
        let key: String;
        match self.it.next() {
            None => return None,
            Some(r) => {
                if self.settings.reduce_group_insensitive {
                    key = r.key[..].to_ascii_lowercase();
                } else {
                    key = r.key
                }
                collection.push(r.value)
            }
        }
        loop {
            match self.it.peek() {
                None => break,
                Some(r) => {
                    if !self.settings.reduce_group_insensitive && r.key != key {
                        break;
                    } else if self.settings.reduce_group_insensitive &&
                       r.key[..].to_ascii_lowercase() != key {
                        break;
                    }
                }
            }
            collection.push(self.it.next().unwrap().value);
        }
        return Some(MultiRecord::new(key, collection));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use parameters::MRParameters;
    use record_types::*;
    use std::vec;

    fn get_records() -> Vec<Record> {
        vec![mk_rcrd("aaa", "def"),
             mk_rcrd("abb", "111"),
             mk_rcrd("Abb", "112"),
             mk_rcrd("abbb", "113"),
             mk_rcrd("abc", "xyz"),
             mk_rcrd("xyz", "___"),
             mk_rcrd("xyz", "__foo"),
             mk_rcrd("xyz", "---")]
    }

    #[test]
    fn test_grouping_iterator() {
        let records = get_records();
        let group_it: RecordsToMultiRecords<vec::IntoIter<Record>> =
            RecordsToMultiRecords::new(records.into_iter(),
                                       MRParameters::new().set_reduce_group_opts(2, true));

        let lengths = vec![1, 2, 1, 1, 3];
        let mut i = 0;

        for multirec in group_it {
            assert_eq!(multirec.into_iter().count(), lengths[i]);
            i += 1;
        }
    }

    #[test]
    fn test_grouping_iterator_sensitive() {
        let records = get_records();
        let group_it: RecordsToMultiRecords<vec::IntoIter<Record>> =
            RecordsToMultiRecords::new(records.into_iter(),
                                       MRParameters::new().set_reduce_group_opts(2, false));

        let lengths = vec![1, 1, 1, 1, 1, 3];
        let mut i = 0;

        for multirec in group_it {
            assert_eq!(multirec.into_iter().count(), lengths[i]);
            i += 1;
        }
    }
}
