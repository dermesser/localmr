use std::cmp::{Eq, PartialEq, Ordering, PartialOrd};
use std::collections::LinkedList;

/// A (key,value) pair.
#[derive(Clone, PartialEq, Eq)]
pub struct Record {
    pub key: String,
    pub value: String,
}

impl PartialOrd for Record {
    fn partial_cmp(&self, other: &Record) -> Option<Ordering> {
        match self.key.cmp(&other.key) {
            Ordering::Equal => Some(self.value.cmp(&other.value)),
            o => Some(o)
        }
    }
}

/// A (key,[value]) pair; typicall used as input to a reducer function.
/// Can be easily iterated over, e.g. in a `for` loop.
pub struct MultiRecord {
    key: String,
    value: Box<Iterator<Item = String>>,
}

impl MultiRecord {
    /// Retrieves the key of the record.
    pub fn key<'a>(&'a self) -> &'a String {
        &self.key
    }
}

impl PartialEq for MultiRecord {
    fn eq(&self, other: &MultiRecord) -> bool {
        self.key == other.key
    }
}

impl PartialOrd for MultiRecord {
    fn partial_cmp(&self, other: &MultiRecord) -> Option<Ordering> {
        Some(self.key.cmp(&other.key))
    }
}

impl IntoIterator for MultiRecord {
    type Item = String;
    type IntoIter = Box<Iterator<Item=String>>;
    /// Allows iterating over all the values.
    fn into_iter(self) -> Self::IntoIter {
        self.value
    }
}

/// Emitter type used in the mapper phase; used to emit (key,value) pairs.
pub struct MEmitter {
    r: LinkedList<Record>,
}

impl MEmitter {
    pub fn new() -> MEmitter {
        MEmitter { r: LinkedList::new() }
    }
    pub fn emit(&mut self, key: String, val: String) {
        self.r.push_back(Record {
            key: key,
            value: val,
        })
    }
    pub fn _get(self) -> LinkedList<Record> {
        self.r
    }
}

/// Emitter used in the reducer phase; used to emit values.
pub struct REmitter {
    r: LinkedList<String>,
}

impl REmitter {
    pub fn new() -> REmitter {
        REmitter { r: LinkedList::new() }
    }
    pub fn emit(&mut self, val: String) {
        self.r.push_back(val)
    }
    pub fn _get(self) -> LinkedList<String> {
        self.r
    }
}
