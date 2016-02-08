use std::collections::linked_list;
use std::collections::LinkedList;
use std::vec;

use record_types::Record;

/// Holds inputs, e.g. to the Map phase, in memory.
/// Specialty: Holding large amounts in memory in a way that is both efficient to store and
/// efficient to iterate.
pub struct InputCache {
    chunks_iter: linked_list::IntoIter<Vec<Record>>,
    chunk_iter: vec::IntoIter<Record>,
    len: usize,
}

impl InputCache {
    pub fn from_iter<It: IntoIterator<Item = Record>>(chunk_length: usize,
                                                      max_bytes: usize,
                                                      it: It)
                                                      -> Self {
        let mut chunklist = LinkedList::new();
        let mut chunk = Vec::with_capacity(chunk_length);

        let mut i: usize = 0;
        let mut complete_length: usize = 0;
        let mut bytes_read: usize = 0;

        for v in it {
            i += 1;
            complete_length += 1;
            bytes_read += v.key.len() + v.value.len();

            chunk.push(v);

            if i >= chunk_length {
                chunklist.push_back(chunk);
                chunk = Vec::with_capacity(chunk_length);
                i = 0;
            }
            if bytes_read >= max_bytes {
                break;
            }
        }

        if chunk.len() > 0 {
            chunklist.push_back(chunk);
        }

        if chunklist.len() == 0 {
            InputCache {
                len: 0,
                chunks_iter: LinkedList::new().into_iter(),
                chunk_iter: Vec::new().into_iter(),
            }
        } else {
            let first_chunk_iterator = chunklist.pop_front().unwrap().into_iter();
            InputCache {
                len: complete_length,
                chunks_iter: chunklist.into_iter(),
                chunk_iter: first_chunk_iterator,
            }
        }
    }

    pub fn len(&self) -> usize {
        self.len
    }
}

impl Iterator for InputCache {
    type Item = Record;
    fn next(&mut self) -> Option<Self::Item> {
        match self.chunk_iter.next() {
            None => (),
            Some(v) => return Some(v),
        }
        match self.chunks_iter.next() {
            None => (),
            Some(chunk) => {
                self.chunk_iter = chunk.into_iter();
                return self.chunk_iter.next();
            }
        }
        None
    }
}
