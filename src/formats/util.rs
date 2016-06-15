//! Various iterators/adapters used for input/output formats.


use record_types::Record;
use std::fmt;

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
