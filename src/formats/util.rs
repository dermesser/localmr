//! Various iterators/adapters used for input/output formats.


use record_types::Record;
use std::fmt;
use std::io;

/// Transforms an iterator<string> into an iterator<Record>. It yields
/// records with the key being the position of the current record, starting with
/// 1. Mainly used as input iterator in the mapping phase, from sources that only
/// yield values (no keys).
pub struct PosRecordIterator<I: Iterator<Item = String>> {
    i: I,
    counter: u64,
}

impl<I: Iterator<Item = String>> PosRecordIterator<I> {
    pub fn new(it: I) -> PosRecordIterator<I> {
        PosRecordIterator {
            i: it,
            counter: 0,
        }
    }
}

impl<I: Iterator<Item = String>> Iterator for PosRecordIterator<I> {
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
pub struct RecordReadIterator<I: Iterator<Item = String>> {
    i: I,
}

impl<I: Iterator<Item = String>> RecordReadIterator<I> {
    pub fn new(it: I) -> RecordReadIterator<I> {
        RecordReadIterator { i: it }
    }
}

impl<I: Iterator<Item = String>> Iterator for RecordReadIterator<I> {
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

/// A type implementing SinkGenerator is used at the end of the reducer
/// phase to write the output. Given a name, new() should return a new object
/// that can be used to write the output of a reduce partition.
/// Values are always written as a whole to the writer.
///
/// SinkGenerator types are used in general to determine the format of outputs; existing options
/// are plain text files (LinesSinkGenerator) or length-prefixed binary files (WriteLogGenerator).
pub trait SinkGenerator: Send + Clone {
    type Sink: io::Write;
    /// Return a new output identified by name. The existing sink generators use `name` to open
    /// files with that name (or path).
    fn new_output(&self, name: &String) -> Self::Sink;
}
