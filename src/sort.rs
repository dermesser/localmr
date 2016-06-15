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

/// Compares a with b in dictionary order (case insensitive)
#[inline]
pub fn sane_string_compare(a: &String, b: &String) -> Ordering {
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::cmp::Ordering;

    #[test]
    fn test_sane_string_compare() {
        let cnv = String::from;
        let s1 = &cnv("");
        let s2 = &cnv("0abc");
        let s3 = &cnv("123");
        let s4 = &cnv("abc");
        let s5 = &cnv("Abc");
        let s6 = &cnv("ABC");
        let s7 = &cnv("aBC");

        assert_eq!(sane_string_compare(s1, s2), Ordering::Less);
        assert_eq!(sane_string_compare(s2, s3), Ordering::Less);
        assert_eq!(sane_string_compare(s3, s2), Ordering::Greater);
        assert_eq!(sane_string_compare(s2, s2), Ordering::Equal);
        assert_eq!(sane_string_compare(s4, s5), Ordering::Less);
        assert_eq!(sane_string_compare(s5, s6), Ordering::Less);
        assert_eq!(sane_string_compare(s6, s7), Ordering::Greater);
    }

    #[inline]
    fn bogus_fn(o: Ordering) -> bool {
        if o == Ordering::Equal {
            panic!("bogus panic")
        }
        true
    }

    // Slow test!
    // #[test]
    fn bench_sane_string_compare() {
        let cnv = String::from;
        let s1 = &cnv("");
        let s2 = &cnv("0abc");
        let s3 = &cnv("123");
        let s4 = &cnv("abc");
        let s5 = &cnv("Abc");

        for _ in 0..50000000 {
            bogus_fn(sane_string_compare(s1, s2));
            bogus_fn(sane_string_compare(s4, s3));
            bogus_fn(sane_string_compare(s4, s5));
        }
    }
}
