//! Implements the Map phase.
//!

#![allow(dead_code)]

use std::collections::BTreeMap;
use std::fmt;
use std::io::Write;

use formats::util::MRSinkGenerator;
use mapreducer::MapReducer;
use parameters::MRParameters;
use record_types::{Record, MEmitter};

/// This is the base of the mapping phase. It contains an input
/// and intermediary input and output forms.
/// Mapper threads run on this. Every mapper thread has one MapPartition
/// instance per input chunk.
struct MapPartition<MR: MapReducer, MapInput: Iterator<Item = Record>, SinkGen: MRSinkGenerator> {
    mr: MR,
    params: MRParameters,
    input: MapInput,
    sink: SinkGen,
    sorted_input: BTreeMap<String, String>,
    sorted_output: BTreeMap<String, Vec<String>>,
}

impl<MR: MapReducer, MapInput: Iterator<Item=Record>, SinkGen: MRSinkGenerator> MapPartition<MR, MapInput, SinkGen> {
    pub fn _new(params: MRParameters,
                input: MapInput,
                mr: MR,
                output: SinkGen)
                -> MapPartition<MR, MapInput, SinkGen> {
        MapPartition {
            mr: mr,
            params: params,
            input: input,
            sink: output,
            sorted_input: BTreeMap::new(),
            sorted_output: BTreeMap::new(),
        }
    }
    pub fn _run(mut self) {
        self.sort_input();
        self.do_map();
        self.write_output();
    }

/// Sorts input into the sorted_input map, moving the records on the way
/// (so no copying happens and memory consumption stays low-ish)
    fn sort_input(&mut self) {
        loop {
            match self.input.next() {
                None => break,
                Some(record) => {
                    self.sorted_input.insert(record.key, record.value);
                }
            }
        }
    }

/// Executes the mapping phase.
    fn do_map(&mut self) {
        let mut key_buffer = Vec::with_capacity(self.params.key_buffer_size);

        loop {
            for k in self.sorted_input.keys().take(self.params.key_buffer_size) {
                key_buffer.push(k.clone())
            }

            for k in &key_buffer[..] {
                let val;
                match self.sorted_input.remove(k) {
                    None => continue,
                    Some(v) => val = v,
                }
                let mut e = MEmitter::new();
                self.mr.map(&mut e,
                            Record {
                                key: k.clone(),
                                value: val,
                            });
                self.insert_result(e);
            }

            if key_buffer.len() < self.params.key_buffer_size {
                break;
            }
            key_buffer.clear();
        }
    }

    fn setup_output(&mut self) -> Vec<SinkGen::Sink> {
// Set up sharded outputs.
        let mut outputs = Vec::new();

        for i in 0..self.params.reducers {
            let out = self.sink.new_output(&fmt::format(format_args!("mapout_{}.{}",
                                                                     self.params.shard_id,
                                                                     i)));
            outputs.push(out);
        }
        assert_eq!(outputs.len(), self.params.reducers);
        outputs
    }

    fn write_output(&mut self) {
        let mut outputs = self.setup_output();

        for (k, vs) in self.sorted_output.iter() {
            let shard = self.mr.shard(self.params.reducers, k);

            for v in vs {
                let r1 = outputs[shard].write(k.as_bytes());
                match r1 {
                    Err(e) => panic!("couldn't write map output: {}", e),
                    Ok(_) => (),
                }
                let r2 = outputs[shard].write(v.as_bytes());
                match r2 {
                    Err(e) => panic!("couldn't write map output: {}", e),
                    Ok(_) => (),
                }
            }
        }
    }

    fn insert_result(&mut self, emitter: MEmitter) {
        for r in emitter._get() {
            let e;
            {
                e = self.sorted_output.remove(&r.key);
            }

            match e {
                None => {
                    self.sorted_output.insert(r.key, vec![r.value]);
                }
                Some(mut v) => {
                    v.push(r.value);
                    self.sorted_output.insert(r.key, v);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use closure_mr::ClosureMapReducer;
    use formats::util::RecordIterator;
    use formats::lines::LinesSinkGenerator;
    use map::MapPartition;
    use record_types::{MEmitter, REmitter, Record, MultiRecord};
    use parameters::MRParameters;
    use std::collections::LinkedList;

    fn mapper_func(e: &mut MEmitter, r: Record) {
        for w in r.value.split_whitespace() {
            e.emit(String::from(w), String::from("1"));
        }
    }

    fn reducer_func(_: &mut REmitter, _: MultiRecord) {
        // no-op
    }

    fn get_mr() -> ClosureMapReducer {
        ClosureMapReducer::new(mapper_func, reducer_func)
    }

    fn get_input() -> LinkedList<Record> {
        let inp: Vec<String> = vec!["abc def",
                                    "xy yz za",
                                    "hello world",
                                    "let's do this",
                                    "foo bar baz"]
                                   .iter()
                                   .map(move |s| String::from(*s))
                                   .collect();
        let ri: RecordIterator<_> = RecordIterator::new(inp.into_iter());
        ri.collect()
    }


    fn get_output() -> LinesSinkGenerator {
        LinesSinkGenerator::new_to_files(&String::from("test_map_"))
    }

    #[test]
    fn test_map_partition() {
        use std::fmt::format;
        use std::fs;

        let reducers = 3;
        let mp = MapPartition::_new(MRParameters::new().set_concurrency(4, reducers),
                                    get_input().into_iter(),
                                    get_mr(),
                                    get_output());
        mp._run();

        for i in 0..reducers {
            let filename = format(format_args!("test_map_mapout_0.{}", i));
            let _ = fs::remove_file(filename);
        }
    }
}
