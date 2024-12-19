use anyhow::Result;
use std::cmp::Ordering;

use super::StorageIterator;

enum Current {
    A,
    B,
}

/// Merges two iterators of different types into one. If the two iterators have the same key, only
/// produce the key once and prefer the entry from A.
pub struct TwoMergeIterator<A: StorageIterator, B: StorageIterator> {
    a: A,
    b: B,
    current: Option<Current>,
}

impl<
        A: 'static + StorageIterator,
        B: 'static + for<'a> StorageIterator<KeyType<'a> = A::KeyType<'a>>,
    > TwoMergeIterator<A, B>
{
    pub fn create(a: A, b: B) -> Result<Self> {
        let mut iter = Self {
            a,
            b,
            current: None,
        };
        iter.set_current()?;
        Ok(iter)
    }

    fn set_current(&mut self) -> Result<()> {
        if !self.is_valid() {
            return Ok(());
        }

        if !self.b.is_valid() {
            self.current = Some(Current::A);
            return Ok(());
        } else if !self.a.is_valid() {
            self.current = Some(Current::B);
            return Ok(());
        }

        match self.a.key().cmp(&self.b.key()) {
            Ordering::Less | Ordering::Equal => self.current = Some(Current::A),
            Ordering::Greater => self.current = Some(Current::B),
        }
        if self.a.key() == self.b.key() {
            // TODO: possible to do this above?
            println!("set_current() - keys equal, advancing b");
            self.b.next()?; // same key can't exist twice in B so no need to recurse
        }
        Ok(())
    }
}

impl<
        A: 'static + StorageIterator,
        B: 'static + for<'a> StorageIterator<KeyType<'a> = A::KeyType<'a>>,
    > StorageIterator for TwoMergeIterator<A, B>
{
    type KeyType<'a> = A::KeyType<'a>;

    fn key(&self) -> Self::KeyType<'_> {
        match self.current {
            Some(Current::A) => self.a.key(),
            Some(Current::B) => self.b.key(),
            _ => {
                panic!("Current iterator not set")
            }
        }
    }

    fn value(&self) -> &[u8] {
        match self.current {
            Some(Current::A) => self.a.value(),
            Some(Current::B) => self.b.value(),
            _ => {
                panic!("Current iterator not set")
            }
        }
    }

    fn is_valid(&self) -> bool {
        self.a.is_valid() || self.b.is_valid()
    }

    fn next(&mut self) -> Result<()> {
        match self.current {
            Some(Current::A) => self.a.next()?,
            Some(Current::B) => self.b.next()?,
            _ => {}
        }
        self.set_current()?;
        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        self.a.num_active_iterators() + self.b.num_active_iterators()
    }
}
