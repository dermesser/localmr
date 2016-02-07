//! Implements the Reduce phase.
//!

use std::io;
use std::iter::Peekable;

use mapreducer::MapReducer;
use parameters::MRParameters;
use record_types::{Record, MultiRecord, REmitter};
use shard_merge::ShardMergeIterator;

pub struct ReducePartition<MR: MapReducer,
                           InputIt: Iterator<Item = Record>,
                           Sink: io::Write>
{
    mr: MR,
    params: MRParameters,
    // Maybe we want to genericize this to an Iterator<Item=Read> or so? This defers opening
    // the files to the reduce shard itself.
    srcs: Vec<InputIt>,
    dstfile: Sink,
}

impl<MR: MapReducer, InputIt: Iterator<Item=Record>, Sink: io::Write> ReducePartition<MR, InputIt, Sink> {
/// Create a new Reduce partition for the given MR; source and destination I/O.
/// mr is the map/reduce functions.
/// params is generic MR parameters as well as some applying directly to this reduce partition.
/// srcs is a set of Iterator<Item=Record>s. Those are usually reading from the map phase's
/// outputs.
/// dstfiles is a Sink (as known from the mapping phase) that is used to create the output
/// file (there is one output file per reduce partition, currently).
    pub fn new(mr: MR, params: MRParameters, srcs: Vec<InputIt>, outp: Sink) -> ReducePartition<MR, InputIt, Sink> {
        ReducePartition { mr: mr, params: params, srcs: srcs, dstfile: outp}
    }

/// Run the Reduce partition.
    pub fn _run(mut self) {
        let mut inputs = Vec::new();
        inputs.append(&mut self.srcs);
        let mut it = inputs.into_iter();

        let params = self.params.clone();

        self.reduce(RecordsToMultiRecords::new(ShardMergeIterator::build(&mut it), params))
    }

    fn reduce<RecIt: Iterator<Item=Record>>(mut self, inp: RecordsToMultiRecords<RecIt>) {
        use std::io::Write;

        for multirec in inp {
            let mut emitter = REmitter::new();
            self.mr.reduce(&mut emitter, multirec);

            for result in emitter._get().into_iter() {
                match self.dstfile.write(result.as_bytes()) {
                    Err(e) => println!("WARN: While reducing shard #{}: {}", self.params.shard_id, e),
                    Ok(_) => ()
                }
            }
        }
    }
}

/// Iterator adapter: Converts an Iterator<Item=Record> into an Iterator<Item=MultiRecord> by
/// grouping subsequent records with identical key.
/// The original iterator must yield records in sorted order (or at least in an order where
/// identical items are adjacent).
pub struct RecordsToMultiRecords<It: Iterator<Item = Record>> {
    it: Peekable<It>,
    params: MRParameters,
}

impl<It: Iterator<Item = Record>> RecordsToMultiRecords<It> {
    fn new(it: It, params: MRParameters) -> RecordsToMultiRecords<It> {
        RecordsToMultiRecords {
            it: it.peekable(),
            params: params,
        }
    }
}

impl<It: Iterator<Item = Record>> Iterator for RecordsToMultiRecords<It> {
    type Item = MultiRecord;
    fn next(&mut self) -> Option<Self::Item> {
        use std::ascii::AsciiExt;
        let mut collection = Vec::with_capacity(self.params.reduce_group_prealloc_size);
        let key: String;
        match self.it.next() {
            None => return None,
            Some(r) => {
                if self.params.reduce_group_insensitive {
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
                    if !self.params.reduce_group_insensitive && r.key != key {
                        break;
                    } else if self.params.reduce_group_insensitive &&
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

    use closure_mr::ClosureMapReducer;
    use formats::lines::LinesSinkGenerator;
    use formats::util::SinkGenerator;
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

    fn test_reducer(e: &mut REmitter, recs: MultiRecord) {
        use std::fmt::Write;
        use std::borrow::Borrow;

        let mut out = String::with_capacity(32);
        let _ = out.write_fmt(format_args!("{}:", recs.key()));

        for val in recs {
            let _ = out.write_str(" ");
            let _ = out.write_str(val.borrow());
        }

        e.emit(out);
    }

    fn fake_mapper(_: &mut MEmitter, _: Record) {}

    #[test]
    fn test_reduce() {
        let mr = ClosureMapReducer::new(fake_mapper, test_reducer);
        let params = MRParameters::new()
                         .set_shard_id(42)
                         .set_reduce_group_opts(1, true)
                         .set_file_locations(String::from("testdata/map_intermed_"),
                                             String::from("testdata/result_"));
        let srcs = vec![get_records().into_iter()];
        let dst = LinesSinkGenerator::new_to_files();

        let r = ReducePartition::new(mr, params, srcs, dst.new_output(&String::from("0")));
        r._run();
    }
}
