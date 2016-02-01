//! Implements a merge tree to merge an arbitrary number of sorted map outputs. See
//! https://drive.google.com/open?id=1grB87a0w9fQ2k7i04N3VJvYlw2BldxWNcHublW_ygJs.
//! Genericized in order to build arbitrary merge trees.

use std::cmp::PartialOrd;
use std::iter;

/// See module description.
/// This type uses dynamic instead of static dispatch because it realizes an arbitrary structure
/// and can therefore not work with a single type signature.
pub struct ShardMergeIterator<'a, T: PartialOrd> {
    left: Box<Iterator<Item=T> + 'a>,
    right: Box<Iterator<Item=T> + 'a>,

    left_peeked: Option<T>,
    right_peeked: Option<T>,
}

impl<'a, T: PartialOrd + Clone> Iterator for ShardMergeIterator<'a, T> {
    type Item = T;
    fn next(&mut self) -> Option<Self::Item> {
        // fill up
        match (self.left_peeked.clone(), self.right_peeked.clone()) {
            (None, None) => { self.left_peeked = self.left.next(); self.right_peeked = self.right.next() },
            (Some(_), None) => self.right_peeked = self.right.next(),
            (None, Some(_)) => self.left_peeked = self.left.next(),
            (Some(_), Some(_)) => ()
        }

        // Consume peeked values
        match (self.left_peeked.clone(), self.right_peeked.clone()) {
            (None, None) => { return None },
            (l @ Some(_), None) => { self.left_peeked = None; return l },
            (None, r @ Some(_)) => { self.right_peeked = None; return r },
            (l @ Some(_), r @ Some(_)) => {
                if l <= r {
                    self.left_peeked = None;
                    return l
                } else {
                    self.right_peeked = None;
                    return r
                }
            }
        }
    }
}

impl<'a, T: PartialOrd + Clone> ShardMergeIterator<'a, T> {
    fn default() -> ShardMergeIterator<'a, T> where T: 'a {
        ShardMergeIterator {
            left: Box::new(iter::empty()),
            right: Box::new(iter::empty()),
            left_peeked: None,
            right_peeked: None,
        }
    }
    /// Takes multiple iterators of type It and generates one ShardedMergeIterator..
    /// (yes, iterator over a collection of iterators).
    pub fn build<It: Iterator<Item=T>, ItIt: Iterator<Item=It>>(sources: &mut ItIt) -> ShardMergeIterator<'a, T>
        where T: 'a, It: 'a {
            let mut merged: Vec<ShardMergeIterator<T>> = Vec::new();

            // Initial merging: Merge pairs of input iterators together.
            loop {
                let src1: It;
                match sources.next() {
                    None => break,
                    Some(src) => src1 = src,
                }
                match sources.next() {
                    None => merged.push(ShardMergeIterator { left: Box::new(src1), right: Box::new(iter::empty()), .. ShardMergeIterator::default() }),
                    Some(src) => merged.push(ShardMergeIterator { left: Box::new(src1), right: Box::new(src), .. ShardMergeIterator::default() }),
                }
            }

            // Recursively build the merge tree from the leaves.
            ShardMergeIterator::merge(merged)
    }

    /// Merge multiple ShardMergeIterators, recursively (meaning it will result in a more or less
    /// balanced merge sort tree).
    fn merge(mut its: Vec<ShardMergeIterator<'a, T>>) -> ShardMergeIterator<'a, T> where T: 'a {
        if its.len() == 0 {
            ShardMergeIterator::default()
        } else if its.len() == 1 {
            ShardMergeIterator { left: Box::new(its.remove(0)), .. ShardMergeIterator::default() }
        } else if its.len() == 2 {
            let it1 = its.remove(0);
            let it2 = its.remove(0);
            ShardMergeIterator { left: Box::new(it1), right: Box::new(it2), ..ShardMergeIterator::default() }
        } else {
            // its is left part, right is right part
            let split_at = its.len() / 2;
            let right = its.split_off(split_at);
            ShardMergeIterator { left: Box::new(ShardMergeIterator::merge(its)), right: Box::new(ShardMergeIterator::merge(right)), .. ShardMergeIterator::default() }
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
        let it = ShardMergeIterator::build(&mut vec![get_collection_1(), get_collection_2(), get_collection_3(),
            get_collection_4(), get_collection_5(), get_collection_6()].into_iter());
        let mut cmp = 0;
        let mut cnt = 0;

        for i in it {
            assert!(i >= cmp);
            cmp = i;
            cnt += 1;
        }

        assert_eq!(cnt, get_collection_1().len()+get_collection_2().len()+get_collection_3().len()+
                   get_collection_4().len()+get_collection_5().len()+get_collection_6().len());
    }
}
