use std::collections::LinkedList;
use std::clone::Clone;

pub struct Record {
    pub key: String,
    pub value: String,
}

/// Input to a reducer function.
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

impl IntoIterator for MultiRecord {
    type Item = String;
    type IntoIter = Box<Iterator<Item=String>>;
    /// Allows iterating over all the values.
    fn into_iter(self) -> Self::IntoIter {
        self.value
    }
}

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

pub type MapperF = fn(&mut MEmitter, Record);
pub type ReducerF = fn(&mut REmitter, MultiRecord);

/// A type implementing map() and reduce() functions.
pub trait MapReducer: Clone {
    /// Takes one <key,value> pair and an emitter.
    /// The emitter is used to yield results from the map phase.
    fn map(&self, em: &mut MEmitter, record: Record);
    fn reduce(&self, em: &mut REmitter, records: MultiRecord);
}
