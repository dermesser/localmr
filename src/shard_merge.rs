//! Implements a merge tree to merge an arbitrary number of sorted map outputs. See
//! https://drive.google.com/open?id=1grB87a0w9fQ2k7i04N3VJvYlw2BldxWNcHublW_ygJs.
//! Genericized in order to build arbitrary merge trees.

#![allow(dead_code)]

use sort;

use std::cmp::{Ord, Ordering};
use std::iter;

/// See module description.
/// This type uses dynamic instead of static dispatch because it realizes an arbitrary structure
/// and can therefore not work with a single type signature.
pub struct ShardMergeIterator<'a, T: Ord> {
    left: Box<Iterator<Item = T> + 'a>,
    right: Box<Iterator<Item = T> + 'a>,

    left_peeked: Option<T>,
    right_peeked: Option<T>,
    comparer: sort::Comparer<T>,
}

impl<'a, T: Ord + Clone> Iterator for ShardMergeIterator<'a, T> {
    type Item = T;
    fn next(&mut self) -> Option<Self::Item> {
        // fill up
        match (self.left_peeked.clone(), self.right_peeked.clone()) {
            (None, None) => {
                self.left_peeked = self.left.next();
                self.right_peeked = self.right.next()
            }
            (Some(_), None) => self.right_peeked = self.right.next(),
            (None, Some(_)) => self.left_peeked = self.left.next(),
            (Some(_), Some(_)) => (),
        }

        // Consume peeked values
        match (self.left_peeked.clone(), self.right_peeked.clone()) {
            (None, None) => return None,
            (l @ Some(_), None) => {
                self.left_peeked = None;
                return l;
            }
            (None, r @ Some(_)) => {
                self.right_peeked = None;
                return r;
            }
            (Some(l), Some(r)) => {
                let cmp = (self.comparer)(&l, &r);
                if cmp == Ordering::Less || cmp == Ordering::Equal {
                    self.left_peeked = None;
                    return Some(l);
                } else {
                    self.right_peeked = None;
                    return Some(r);
                }
            }
        }
    }
}

impl<'a, T: Ord + Clone> ShardMergeIterator<'a, T> {
    fn default() -> ShardMergeIterator<'a, T>
        where T: 'a
    {
        ShardMergeIterator {
            left: Box::new(iter::empty()),
            right: Box::new(iter::empty()),
            left_peeked: None,
            right_peeked: None,
            // BUG: This should not be altered when used with Map phase output.
            // The map phase uses a BTreeMap in order to sort the output, and the BTM
            // only uses the standard Ord implementation for strings. Should the requirements
            // change, we can work around that.
            comparer: sort::default_generic_compare,
        }
    }

    pub fn build<It: Iterator<Item = T>, ItIt: Iterator<Item = It>>(sources: &mut ItIt)
                                                                    -> ShardMergeIterator<'a, T>
        where T: 'a,
              It: 'a
    {
        ShardMergeIterator::_build(sources, None)
    }

    pub fn build_with_cmp<It: Iterator<Item = T>, ItIt: Iterator<Item = It>>
        (sources: &mut ItIt,
         cmp: sort::Comparer<T>)
         -> ShardMergeIterator<'a, T>
        where T: 'a,
              It: 'a
    {
        ShardMergeIterator::_build(sources, Some(cmp))
    }

    /// Takes multiple iterators of type It and generates one ShardedMergeIterator..
    /// (yes, iterator over a collection of iterators).
    fn _build<It: Iterator<Item = T>, ItIt: Iterator<Item = It>>(sources: &mut ItIt,
                                                                 cmp_o: Option<sort::Comparer<T>>)
                                                                 -> ShardMergeIterator<'a, T>
        where T: 'a,
              It: 'a
    {
        let mut merged: Vec<ShardMergeIterator<T>> = Vec::new();
        let cmp_fn = cmp_o.unwrap_or(sort::default_generic_compare);

        // Initial merging: Merge pairs of input iterators together.
        loop {
            let src1: It;
            match sources.next() {
                None => break,
                Some(src) => src1 = src,
            }
            match sources.next() {
                None => {
                    merged.push(ShardMergeIterator {
                        left: Box::new(src1),
                        right: Box::new(iter::empty()),
                        comparer: cmp_fn,
                        ..ShardMergeIterator::default()
                    })
                }
                Some(src) => {
                    merged.push(ShardMergeIterator {
                        left: Box::new(src1),
                        right: Box::new(src),
                        comparer: cmp_fn,
                        ..ShardMergeIterator::default()
                    })
                }
            }
        }

        // Recursively build the merge tree from the leaves.
        ShardMergeIterator::merge(merged, cmp_fn)
    }

    /// Merge multiple ShardMergeIterators, recursively (meaning it will result in a more or less
    /// balanced merge sort tree).
    fn merge(mut its: Vec<ShardMergeIterator<'a, T>>,
             cmp: sort::Comparer<T>)
             -> ShardMergeIterator<'a, T>
        where T: 'a
    {
        if its.len() == 0 {
            ShardMergeIterator::default()
        } else if its.len() == 1 {
            ShardMergeIterator {
                left: Box::new(its.remove(0)),
                comparer: cmp,
                ..ShardMergeIterator::default()
            }
        } else if its.len() == 2 {
            let it1 = its.remove(0);
            let it2 = its.remove(0);
            ShardMergeIterator {
                left: Box::new(it1),
                right: Box::new(it2),
                comparer: cmp,
                ..ShardMergeIterator::default()
            }
        } else {
            // its is left part, right is right part
            let split_at = its.len() / 2;
            let right = its.split_off(split_at);
            ShardMergeIterator {
                left: Box::new(ShardMergeIterator::merge(its, cmp)),
                right: Box::new(ShardMergeIterator::merge(right, cmp)),
                comparer: cmp,
                ..ShardMergeIterator::default()
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::vec;
    use shard_merge::ShardMergeIterator;

    fn get_collection_1() -> vec::IntoIter<i32> {
        vec![1, 4, 5, 5, 6, 9, 11, 15, 15, 17, 18, 20].into_iter()
    }
    fn get_collection_2() -> vec::IntoIter<i32> {
        vec![2, 2, 2, 3, 4, 5, 7, 8, 9, 10, 45, 46, 47].into_iter()
    }
    fn get_collection_3() -> vec::IntoIter<i32> {
        vec![5, 8, 9, 10, 22, 25, 30, 37, 41, 46, 71].into_iter()
    }
    fn get_collection_4() -> vec::IntoIter<i32> {
        vec![111, 112, 113, 155].into_iter()
    }
    fn get_collection_5() -> vec::IntoIter<i32> {
        vec![13, 45, 98, 105, 145].into_iter()
    }
    fn get_collection_6() -> vec::IntoIter<i32> {
        vec![14, 67, 99, 111, 222, 566, 643].into_iter()
    }

    #[test]
    fn test_merge_iterator() {
        let it = ShardMergeIterator::build(&mut vec![get_collection_1(),
                                                     get_collection_2(),
                                                     get_collection_3(),
                                                     get_collection_4(),
                                                     get_collection_5(),
                                                     get_collection_6()]
                                                    .into_iter());
        let mut cmp = 0;
        let mut cnt = 0;

        for i in it {
            assert!(i >= cmp);
            cmp = i;
            cnt += 1;
        }

        assert_eq!(cnt,
                   get_collection_1().len() + get_collection_2().len() +
                   get_collection_3().len() + get_collection_4().len() +
                   get_collection_5().len() + get_collection_6().len());
    }

    use formats::lines;
    use std::fmt;
    use std::io::Write;
    use sort;

    // Slow test!
    // #[test]
    fn test_merge_large_files() {
        let mut files = Vec::with_capacity(11);

        for i in 0..11 {
            let name = fmt::format(format_args!("testdata/sorted{}.txt", i));
            files.push(lines::new_from_file(&name).unwrap());
        }

        let merge_it = ShardMergeIterator::build_with_cmp(&mut files.into_iter(),
                                                          sort::dict_string_compare);
        let mut outfile = lines::LinesWriter::new_to_file(&String::from("testdata/all_sorted.txt"))
                              .unwrap();

        for line in merge_it {
            let _ = outfile.write(line.as_bytes());
        }
    }
}
