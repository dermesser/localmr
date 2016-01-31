//! Various iterators/adapters used for input/output formats.

use mapreducer::Record;
use std::fmt;

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
