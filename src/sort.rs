//! Sorting/comparison functions of various sorts.

#![allow(dead_code)]

use std::cmp::{Ord, Ordering};

/// Function type to be used as custom compare function
/// (rust's standard String comparison is based on ASCII values, not dictionary order)
pub type Comparer<T> = fn(a: &T, b: &T) -> Ordering;

/// Comparer<T: Ord>
#[inline]
pub fn default_generic_compare<T: Ord>(a: &T, b: &T) -> Ordering {
    a.cmp(b)
}

/// Compares a with b in a totally case insensitive manner
/// (like coreutil sort)
#[inline]
pub fn dict_string_compare(a: &String, b: &String) -> Ordering {
    let (mut charsa, mut charsb) = (a.chars(), b.chars());
    loop {
        match (charsa.next(), charsb.next()) {
            (None, None) => return Ordering::Equal,
            (_, None) => return Ordering::Greater,
            (None, _) => return Ordering::Less,
            (Some(ca), Some(cb)) => {
                let cmp = dict_char_compare(ca, cb);
                if cmp != Ordering::Equal {
                    return cmp;
                } else {
                    continue;
                }
            }
        }
    }
}

#[inline]
fn dict_char_compare(a: char, b: char) -> Ordering {
    use std::ascii::AsciiExt;
    // denormalize case to lower case
    a.to_ascii_lowercase().cmp(&b.to_ascii_lowercase())
}

/// A wrapped string that uses a dictionary string comparison as Ord implementation.
#[derive(PartialEq, Eq, Clone)]
pub enum DictComparableString {
    DCS(String),
}

impl DictComparableString {
    pub fn wrap(s: String) -> DictComparableString {
        DictComparableString::DCS(s)
    }
    pub fn unwrap(self) -> String {
        let DictComparableString::DCS(s) = self;
        s
    }
    pub fn as_ref(&self) -> &String {
        let &DictComparableString::DCS(ref s) = self;
        s
    }
}

impl PartialOrd for DictComparableString {
    fn partial_cmp(&self, other: &DictComparableString) -> Option<Ordering> {
        let (&DictComparableString::DCS(ref a), &DictComparableString::DCS(ref b)) = (self, other);
        Some(dict_string_compare(a, b))
    }
}

impl Ord for DictComparableString {
    fn cmp(&self, other: &DictComparableString) -> Ordering {
        let (&DictComparableString::DCS(ref a), &DictComparableString::DCS(ref b)) = (self, other);
        dict_string_compare(a, b)
    }
}
