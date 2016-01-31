//! Implements the mapping phase.
//!

use std::io::Write;
use std::collections::{LinkedList, BTreeMap};
use mapreducer::{Record, MapReducer, MEmitter};

type MapInput = LinkedList<Record>;

/// This is the base of the mapping phase. It contains an input
/// and intermediary input and output forms.
/// Mapper threads run on this. Every mapper thread has one MapPartition
/// instance per input chunk.
struct MapPartition<MR: MapReducer> {
    mr: MR,
    input: MapInput,
    output: Box<Write>,
    sorted_input: BTreeMap<String, String>,
    sorted_output: BTreeMap<String, Vec<String>>,
}

impl<MR: MapReducer> MapPartition<MR> {
    pub fn _new(input: MapInput, mr: MR, output: Box<Write>) -> MapPartition<MR> {
        MapPartition {
            mr: mr,
            input: input,
            output: output,
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
            match self.input.pop_front() {
                None => break,
                Some(record) => { self.sorted_input.insert(record.key, record.value); },
            }
        }
    }

    /// Executes the mapping phase.
    fn do_map(&mut self) {
        // TODO: Make this configurable
        let key_buffer_size: usize = 256;
        let mut key_buffer = Vec::with_capacity(key_buffer_size);

        loop {
            for k in self.sorted_input.keys().take(key_buffer_size) {
                key_buffer.push(k.clone())
            }

            for k in &key_buffer[..] {
                let val;
                match self.sorted_input.remove(k) {
                    None => continue,
                    Some(v) => val = v
                }
                let mut e = MEmitter::new();
                self.mr.map(&mut e, Record { key: k.clone(), value: val });
                self.insert_result(e);
            }

            key_buffer.clear();
        }
    }

    fn write_output(&mut self) {
        for (k, vs) in self.sorted_output.iter() {
            for v in vs {
                let r1 = self.output.write(k.as_bytes());
                match r1 {
                    Err(e) => panic!("couldn't write map output: {}", e),
                    Ok(_) => ()
                }
                let r2 = self.output.write(v.as_bytes());
                match r2 {
                    Err(e) => panic!("couldn't write map output: {}", e),
                    Ok(_) => ()
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
                None => { self.sorted_output.insert(r.key, vec![r.value]); },
                Some(mut v) => { v.push(r.value); self.sorted_output.insert(r.key, v); },
            }
        }
    }
}
