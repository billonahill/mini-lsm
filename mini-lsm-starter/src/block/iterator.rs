use bytes::Bytes;
use std::sync::Arc;

use crate::key::{KeySlice, KeyVec};

use super::{as_u16, Block, SIZEOF_U16};

/// Iterates on a block.
pub struct BlockIterator {
    /// The internal `Block`, wrapped by an `Arc`
    block: Arc<Block>,
    /// The current key, empty represents the iterator is invalid
    key: KeyVec,
    /// the current value range in the block.data, corresponds to the current key
    value_range: (usize, usize),
    /// Current index of the key-value pair, should be in range of [0, num_of_elements)
    idx: usize,
    /// The first key in the block
    first_key: KeyVec,
}

impl BlockIterator {
    fn new(block: Arc<Block>) -> Self {
        Self {
            block,
            key: KeyVec::new(),
            value_range: (0, 0),
            idx: 0,
            first_key: KeyVec::new(),
        }
    }

    /// Creates a block iterator and seek to the first entry.
    pub fn create_and_seek_to_first(block: Arc<Block>) -> Self {
        let mut iter = BlockIterator::new(block);
        iter.seek_to_first();
        iter
    }

    /// Creates a block iterator and seek to the first key that >= `key`.
    pub fn create_and_seek_to_key(block: Arc<Block>, key: KeySlice) -> Self {
        let mut iter = BlockIterator::new(block);
        iter.seek_to_key(key);
        iter
    }

    /// Returns the key of the current entry.
    pub fn key(&self) -> KeySlice {
        self.key.as_key_slice()
    }

    /// Returns the value of the current entry.
    pub fn value(&self) -> &[u8] {
        &self.block.data.as_slice()[self.value_range.0..self.value_range.1]
    }

    /// Returns true if the iterator is valid.
    /// Note: You may want to make use of `key`
    pub fn is_valid(&self) -> bool {
        !self.key.is_empty()
    }

    /// Seeks to the first key in the block.
    pub fn seek_to_first(&mut self) {
        self.idx = 0;
        self.seek(self.idx)
    }

    /// Move to the next key in the block.
    pub fn next(&mut self) {
        if self.idx + 1 >= self.block.offsets.len() {
            self.value_range = (0, 0);
            self.key = KeyVec::new();
            self.idx = 0;
        } else {
            self.idx = self.idx + 1;
            self.seek(self.idx)
        }
    }

    fn seek(&mut self, index: usize) {
        let offset = self.block.offsets[index] as usize;

        let key_overlap_len = as_u16(&self.block.data[offset..offset + SIZEOF_U16]) as usize;
        let rest_key_len =
            as_u16(&self.block.data[offset + SIZEOF_U16..offset + SIZEOF_U16]) as usize;
        // let key_len = as_u16(&self.block.data[offset..offset + SIZEOF_U16]) as usize;
        let key_start_idx = offset + 2 * SIZEOF_U16;
        let mut key_vec = KeyVec::new();
        if !self.first_key.is_empty() && key_overlap_len > 0 {
            key_vec.append(&self.first_key.raw_ref()[..key_overlap_len]);
        }
        key_vec.append(&self.block.data[key_start_idx..key_start_idx + rest_key_len]);
        self.key = key_vec;
        if self.first_key.is_empty() {
            self.first_key = self.key.clone();
        }

        let value_len_start_idx = key_start_idx + rest_key_len;
        let value_len =
            as_u16(&self.block.data[value_len_start_idx..value_len_start_idx + SIZEOF_U16])
                as usize;
        let value_start_idx = (value_len_start_idx + SIZEOF_U16) as usize;
        self.value_range = (value_start_idx, value_start_idx + value_len);
        // println!(
        //     "Seeked to key {:?}",
        //     as_bytes(self.key.for_testing_key_ref())
        // )
    }

    /// Seek to the first key that >= `key`.
    /// Note: You should assume the key-value pairs in the block are sorted when being added by
    /// callers.
    pub fn seek_to_key(&mut self, key: KeySlice) {
        if self.idx == 0 && self.key.is_empty() {
            // never initialized
            self.seek_to_first();
        }
        while self.key() < key && self.is_valid() {
            println!(
                "key {:?} is < passed key {:?}, calling next()",
                as_bytes(self.key().for_testing_key_ref()),
                as_bytes(key.for_testing_key_ref())
            );
            self.next()
        }
    }
}

fn as_bytes(x: &[u8]) -> Bytes {
    Bytes::copy_from_slice(x)
}
