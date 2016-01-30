use mapreducer::Record;
use std::fmt;

/// Transforms an iterator<string> into an iterator<Record>. It yields
/// records with the key being the position of the current record, starting with
/// 1. Mainly used as input iterator in the mapping phase, from sources that only
/// yield values (no keys).
pub struct RecordIterator {
    i: Box<Iterator<Item=String>>,
    counter: u64,
}

impl RecordIterator {
    pub fn new(it: Box<Iterator<Item=String>>) -> RecordIterator {
        RecordIterator { i: it, counter: 0 }
    }
}

impl Iterator for RecordIterator {
    type Item = Record;
    fn next(&mut self) -> Option<Record> {
        match self.i.next() {
            None => None,
            Some(val) => {
                self.counter += 1;
                Some(Record { key: fmt::format(format_args!("{}", self.counter)), value: val })
            }
        }
    }
}
