//! The MapReducer trait and associated types.

use std::collections::LinkedList;
use std::clone::Clone;

/// A (key,value) pair.
pub struct Record {
    pub key: String,
    pub value: String,
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

/// Map() function type. The MEmitter argument is used to emit values from
/// the map() function.
pub type MapperF = fn(&mut MEmitter, Record);
/// Reduce() function type. The REmitter argument is used to emit values
/// from the reduce() function.
pub type ReducerF = fn(&mut REmitter, MultiRecord);

/// A type implementing map() and reduce() functions.
/// The MapReducer is cloned once per mapper/reducer thread.
pub trait MapReducer: Clone {
    /// Takes one <key,value> pair and an emitter.
    /// The emitter is used to yield results from the map phase.
    fn map(&self, em: &mut MEmitter, record: Record);
    /// Takes one key and one or more values and emits one or more
    /// values.
    fn reduce(&self, em: &mut REmitter, records: MultiRecord);
}
