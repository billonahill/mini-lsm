use anyhow::{bail, Result};
use bytes::Bytes;
use std::collections::Bound;
use std::str;

use crate::{
    iterators::{merge_iterator::MergeIterator, StorageIterator},
    mem_table::MemTableIterator,
};
use crate::iterators::two_merge_iterator::TwoMergeIterator;
use crate::table::SsTableIterator;

/// Represents the internal type for an LSM iterator. This type will be changed across the tutorial for multiple times.
type LsmIteratorInner = TwoMergeIterator<MergeIterator<MemTableIterator>, MergeIterator<SsTableIterator>>;

pub struct LsmIterator {
    inner: LsmIteratorInner,
    is_valid: bool,
    end_bound: Bound<Bytes>,
}

impl LsmIterator {
    pub(crate) fn new(iter: LsmIteratorInner, end_bound: Bound<Bytes>) -> Result<Self> {
        let mut iter = Self {
            is_valid: iter.is_valid(),
            inner: iter,
            end_bound: end_bound,
        };
        iter.move_to_non_delete()?;
        Ok(iter)
    }

    fn next_inner(&mut self) -> Result<()> {
        self.inner.next()?;
        println!(
            "next_inner. Current key:value is now {:?}:{:?}, is_empty: {:?}",
            str::from_utf8(self.key()).unwrap(),
            str::from_utf8(self.value()).unwrap(),
            self.inner.value().is_empty()
        );

        if !self.inner.is_valid() {
            self.is_valid = false;
            return Ok(());
        }
        match self.end_bound.as_ref() {
            Bound::Unbounded => {}
            Bound::Included(key) => self.is_valid = self.inner.key().raw_ref() <= key.as_ref(),
            Bound::Excluded(key) => self.is_valid = self.inner.key().raw_ref() < key.as_ref(),
        }
        return Ok(());
    }

    fn move_to_non_delete(&mut self) -> Result<()> {
        println!(
            "move_to_non_delete. Checking if current key value is empty {:?}:{:?}",
            str::from_utf8(self.key()).unwrap(),
            str::from_utf8(self.value()).unwrap()
        );
        while self.is_valid() && self.inner.value().is_empty() {
            println!(
                "move_to_non_delete. Current key value is empty {:?}:{:?}",
                str::from_utf8(self.key()).unwrap(),
                str::from_utf8(self.value()).unwrap()
            );
            self.next_inner()?;
        }
        Ok(())
    }
}

impl StorageIterator for LsmIterator {
    type KeyType<'a> = &'a [u8];

    fn is_valid(&self) -> bool {
        self.is_valid
    }

    fn key(&self) -> &[u8] {
        self.inner.key().raw_ref()
    }

    fn value(&self) -> &[u8] {
        self.inner.value()
    }

    fn next(&mut self) -> Result<()> {
        println!("LsmIterator.next called");
        println!(
            "LsmIterator.next key:value after non-delete: {:?}:{:?}",
            str::from_utf8(self.key()).unwrap(),
            str::from_utf8(self.value()).unwrap()
        );
        self.next_inner()?;
        self.move_to_non_delete()?;
        println!(
            "LsmIterator.next key:value after next_inner: {:?}:{:?}",
            str::from_utf8(self.key()).unwrap(),
            str::from_utf8(self.value()).unwrap()
        );
        Ok(())
    }
}

/// A wrapper around existing iterator, will prevent users from calling `next` when the iterator is
/// invalid. If an iterator is already invalid, `next` does not do anything. If `next` returns an error,
/// `is_valid` should return false, and `next` should always return an error.
pub struct FusedIterator<I: StorageIterator> {
    iter: I,
    has_errored: bool,
}

impl<I: StorageIterator> FusedIterator<I> {
    pub fn new(iter: I) -> Self {
        Self {
            iter,
            has_errored: false,
        }
    }
}

impl<I: StorageIterator> StorageIterator for FusedIterator<I> {
    type KeyType<'a>
        = I::KeyType<'a>
    where
        Self: 'a;

    fn is_valid(&self) -> bool {
        !self.has_errored && self.iter.is_valid()
    }

    fn key(&self) -> Self::KeyType<'_> {
        if !self.is_valid() {
            panic!("The iterator is not valid");
        }
        self.iter.key()
    }

    fn value(&self) -> &[u8] {
        if !self.is_valid() {
            panic!("The iterator is not valid");
        }
        self.iter.value()
    }

    fn next(&mut self) -> Result<()> {
        if self.has_errored {
            bail!("The iterator is no good");
        }
        if self.iter.is_valid() {
            if let Err(e) = self.iter.next() {
                self.has_errored = true;
                return Err(e);
            }
        }
        return Ok(());
    }
}
