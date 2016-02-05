//! Sorting/comparison functions of various sorts.

#![allow(dead_code)]

use std::cmp::Ordering;

/// Function type to be used as custom compare function
/// (rust's standard String comparison is based on ASCII values, not dictionary order)
pub type Comparer<T> = fn(a: &T, b: &T) -> Ordering;

#[inline]
fn sane_char_compare(a: char, b: char) -> Ordering {
    use std::ascii::AsciiExt;
    // denormalize case to lower case
    let cmp = a.to_ascii_lowercase().cmp(&b.to_ascii_lowercase());

    // Handle the case of a and b being the same letter but with different case.
    // Native: 'B' < 'a'; we want: 'B' > 'a'
    if cmp == Ordering::Equal {
        match a.cmp(&b) {
            Ordering::Equal => Ordering::Equal,  // actually same character
            Ordering::Less => Ordering::Greater, // 'B' > 'a'!
            Ordering::Greater => Ordering::Less, // 'a' < 'B'!
        }
    } else {
        cmp
    }
}

#[inline]
fn dict_char_compare(a: char, b: char) -> Ordering {
    use std::ascii::AsciiExt;
    // denormalize case to lower case
    a.to_ascii_lowercase().cmp(&b.to_ascii_lowercase())
}

/// Compares a with b in a totally case insensitive manner
/// (like coreutil sort)
#[inline]
fn dict_string_compare(a: &String, b: &String) -> Ordering {
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

/// Compares a with b in dictionary order (case insensitive)
#[inline]
fn sane_string_compare(a: &String, b: &String) -> Ordering {
    let (mut charsa, mut charsb) = (a.chars(), b.chars());
    loop {
        match (charsa.next(), charsb.next()) {
            (None, None) => return Ordering::Equal,
            (_, None) => return Ordering::Greater,
            (None, _) => return Ordering::Less,
            (Some(ca), Some(cb)) => {
                let cmp = sane_char_compare(ca, cb);
                if cmp != Ordering::Equal {
                    return cmp;
                } else {
                    continue;
                }
            }
        }
    }
}
