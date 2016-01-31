//! Various iterators/adapters used for input/output formats.


use mapreducer::Record;
use std::fmt;
use std::io;

/// Transforms an iterator<string> into an iterator<Record>. It yields
/// records with the key being the position of the current record, starting with
/// 1. Mainly used as input iterator in the mapping phase, from sources that only
/// yield values (no keys).
pub struct RecordIterator<I: Iterator<Item = String>> {
    i: I,
    counter: u64,
}

impl<I: Iterator<Item = String>> RecordIterator<I> {
    pub fn new(it: I) -> RecordIterator<I> {
        RecordIterator {
            i: it,
            counter: 0,
        }
    }
}

impl<I: Iterator<Item = String>> Iterator for RecordIterator<I> {
    type Item = Record;
    fn next(&mut self) -> Option<Record> {
        match self.i.next() {
            None => None,
            Some(val) => {
                self.counter += 1;
                Some(Record {
                    key: fmt::format(format_args!("{}", self.counter)),
                    value: val,
                })
            }
        }
    }
}

/// Another transformation of [string] -> [(string,string)]; however,
/// this one always reads one value, treats it as key, and another one,
/// treated as value.
pub struct KVReadIterator<I: Iterator<Item = String>> {
    i: I,
}

impl<I: Iterator<Item = String>> KVReadIterator<I> {
    pub fn new(it: I) -> KVReadIterator<I> {
        KVReadIterator { i: it }
    }
}

impl<I: Iterator<Item = String>> Iterator for KVReadIterator<I> {
    type Item = Record;
    fn next(&mut self) -> Option<Record> {
        let (k, v) = (self.i.next(), self.i.next());
        match (k, v) {
            (None, _) => None,
            (_, None) => None,
            (Some(k_), Some(v_)) => {
                Some(Record {
                    key: k_,
                    value: v_,
                })
            }
        }
    }
}

/// A type implementing MRSinkGenerator is used at the end of the reducer
/// phase to write the output. Given a name, new() should return a new object
/// that can be used to write the output of a reduce partition.
/// Values are always written as a whole to the writer.
pub trait MRSinkGenerator {
    type Sink: io::Write + Sized;
    /// Return a new output.
    fn new_output(&mut self, name: &String) -> Self::Sink;
}
