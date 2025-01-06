#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::path::Path;
use std::sync::Arc;

use anyhow::Result;
use bytes::BufMut;

use super::{BlockMeta, FileObject, SsTable};
use crate::key::Key;
use crate::table::bloom::Bloom;
use crate::{block::BlockBuilder, key::KeySlice, lsm_storage::BlockCache};

const SIZE_OF_U32: usize = size_of::<u32>();

/// Builds an SSTable from key-value pairs.
pub struct SsTableBuilder {
    builder: BlockBuilder,
    first_key: Vec<u8>,
    last_key: Vec<u8>,
    data: Vec<u8>,
    key_hashes: Vec<u32>,
    pub(crate) meta: Vec<BlockMeta>,
    block_size: usize,
}

impl SsTableBuilder {
    /// Create a builder based on target block size.
    pub fn new(block_size: usize) -> Self {
        Self {
            builder: BlockBuilder::new(block_size),
            first_key: vec![],
            last_key: vec![],
            data: vec![],
            meta: vec![],
            key_hashes: vec![],
            block_size,
        }
    }

    /// Adds a key-value pair to SSTable.
    ///
    /// Note: You should split a new block when the current block is full.(`std::mem::replace` may
    /// be helpful here)
    pub fn add(&mut self, key: KeySlice, value: &[u8]) {
        if self.first_key.is_empty() {
            self.first_key = key.to_key_vec().into_inner();
        }

        if self.builder.add(key, value) {
            self.key_hashes.push(farmhash::fingerprint32(key.raw_ref()));
            self.last_key = key.to_key_vec().into_inner();
            return;
        }

        self.finish_block();

        assert!(self.builder.add(key, value));
        self.first_key = key.into_inner().to_vec();
        self.last_key = key.into_inner().to_vec();
    }

    fn finish_block(&mut self) {
        let builder = std::mem::replace(&mut self.builder, BlockBuilder::new(self.block_size));
        let block_bytes = builder.build().encode();
        let block_meta = BlockMeta {
            offset: self.data.len(),
            first_key: Key::from_vec(self.first_key.clone()).into_key_bytes(),
            last_key: Key::from_vec(self.last_key.clone()).into_key_bytes(),
        };
        self.data.extend(block_bytes);
        self.meta.push(block_meta);
    }

    /// Get the estimated size of the SSTable.
    ///
    /// Since the data blocks contain much more data than meta blocks, just return the size of data
    /// blocks here.
    pub fn estimated_size(&self) -> usize {
        let mut block_meta_bytes: Vec<u8> = self.data.clone();
        BlockMeta::encode_block_meta(&self.meta, &mut block_meta_bytes);
        self.data.len() + block_meta_bytes.len() + SIZE_OF_U32
    }

    /// Builds the SSTable and writes it to the given path. Use the `FileObject` structure to manipulate the disk objects.
    pub fn build(
        mut self,
        id: usize,
        block_cache: Option<Arc<BlockCache>>,
        path: impl AsRef<Path>,
    ) -> Result<SsTable> {
        self.finish_block();
        // add the data bytes
        let mut output_bytes: Vec<u8> = self.data.clone();
        // add the block meta and bloom filter
        BlockMeta::encode_block_meta(&self.meta, &mut output_bytes);
        let bloom_size = Bloom::encode_bloom(&self.key_hashes, &mut output_bytes);
        // append the offset of the bloom
        output_bytes.put_u32(bloom_size as u32);
        // append the offset of the meta, which is the data length
        output_bytes.put_u32(self.data.len() as u32);
        let file = FileObject::create(path.as_ref(), output_bytes)?;
        let ss_table = SsTable::open(id, block_cache, file)?;
        Ok(ss_table)
    }

    #[cfg(test)]
    pub(crate) fn build_for_test(self, path: impl AsRef<Path>) -> Result<SsTable> {
        self.build(0, None, path)
    }
}
