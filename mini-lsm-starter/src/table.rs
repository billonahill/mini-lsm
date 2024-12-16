#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

pub(crate) mod bloom;
mod builder;
mod iterator;

use std::fs::File;
use std::path::Path;
use std::sync::Arc;

use anyhow::Result;
pub use builder::SsTableBuilder;
use bytes::{Buf, BufMut};
pub use iterator::SsTableIterator;

use crate::block::{Block, BlockIterator};
use crate::key::{KeyBytes, KeySlice};
use crate::lsm_storage::BlockCache;

use self::bloom::Bloom;

pub(crate) const SIZEOF_U16: usize = std::mem::size_of::<u16>();
pub(crate) const SIZEOF_U32: usize = std::mem::size_of::<u32>();

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlockMeta {
    /// Offset of this data block.
    pub offset: usize,
    /// The first key of the data block.
    pub first_key: KeyBytes,
    /// The last key of the data block.
    pub last_key: KeyBytes,
}

impl BlockMeta {
    /// Encode block meta to a buffer.
    /// You may add extra fields to the buffer,
    /// in order to help keep track of `first_key` when decoding from the same buffer in the future.
    pub fn encode_block_meta(
        block_meta: &[BlockMeta],
        #[allow(clippy::ptr_arg)] // remove this allow after you finish
        buf: &mut Vec<u8>,
    ) {
        let block_meta_len = block_meta.len() as u32;
        buf.put_u32(block_meta_len);
        for bm in block_meta {
            let first_key_vec = &mut bm.first_key.as_key_slice().to_key_vec().into_inner().clone();
            let last_key_vec = &mut bm.last_key.as_key_slice().to_key_vec().into_inner().clone();
            buf.put_u16(first_key_vec.len() as u16);  // last key offset
            buf.put_u16(last_key_vec.len() as u16);  // offset offset
            buf.append(first_key_vec);
            buf.append(last_key_vec);
            buf.put_u32(bm.offset as u32);
            println!("encode first key: {:?}", bm.first_key.for_testing_key_ref());
            println!("encode last key: {:?}", bm.last_key.for_testing_key_ref());
            println!("encode offset: {:?}", bm.offset as u32);
        }
    }

    /// Decode block meta from a buffer.
    pub fn decode_block_meta(mut buf: impl Buf) -> Vec<BlockMeta> {
        let num_blocks = buf.get_u32();
        let mut bm_vec: Vec<BlockMeta> = vec!();
        for i in 0..num_blocks {
            let first_key_length = buf.get_u16() as usize;
            let last_key_length = buf.get_u16() as usize;
            bm_vec.push(
                BlockMeta {
                    first_key: KeyBytes::from_bytes(buf.copy_to_bytes(first_key_length)),
                    last_key: KeyBytes::from_bytes(buf.copy_to_bytes(last_key_length)),
                    offset: buf.get_u32() as usize,
                }
            );
            println!("{:?} decode first key: {:?}", i, bm_vec.last().unwrap().first_key.for_testing_key_ref());
            println!("{:?} decode last key: {:?}", i, bm_vec.last().unwrap().last_key.for_testing_key_ref());
            println!("{:?} decode offset: {:?}", i, bm_vec.last().unwrap().offset);
        }
        bm_vec
    }
}

/// A file object.
pub struct FileObject(Option<File>, u64);

impl FileObject {
    pub fn read(&self, offset: u64, len: u64) -> Result<Vec<u8>> {
        use std::os::unix::fs::FileExt;
        let mut data = vec![0; len as usize];
        self.0
            .as_ref()
            .unwrap()
            .read_exact_at(&mut data[..], offset)?;
        Ok(data)
    }

    pub fn size(&self) -> u64 {
        self.1
    }

    /// Create a new file object (day 2) and write the file to the disk (day 4).
    pub fn create(path: &Path, data: Vec<u8>) -> Result<Self> {
        std::fs::write(path, &data)?;
        File::open(path)?.sync_all()?;
        Ok(FileObject(
            Some(File::options().read(true).write(false).open(path)?),
            data.len() as u64,
        ))
    }

    pub fn open(path: &Path) -> Result<Self> {
        let file = File::options().read(true).write(false).open(path)?;
        let size = file.metadata()?.len();
        Ok(FileObject(Some(file), size))
    }
}

/// An SSTable.
pub struct SsTable {
    /// The actual storage unit of SsTable, the format is as above.
    pub(crate) file: FileObject,
    /// The meta blocks that hold info for data blocks.
    pub(crate) block_meta: Vec<BlockMeta>,
    /// The offset that indicates the start point of meta blocks in `file`.
    pub(crate) block_meta_offset: usize,
    id: usize,
    block_cache: Option<Arc<BlockCache>>,
    first_key: KeyBytes,
    last_key: KeyBytes,
    pub(crate) bloom: Option<Bloom>,
    /// The maximum timestamp stored in this SST, implemented in week 3.
    max_ts: u64,
}

impl SsTable {
    #[cfg(test)]
    pub(crate) fn open_for_test(file: FileObject) -> Result<Self> {
        Self::open(0, None, file)
    }

    /// Open SSTable from a file.
    pub fn open(id: usize, block_cache: Option<Arc<BlockCache>>, file: FileObject) -> Result<Self> {
        let block_meta_offset = (&file.read(file.size() - 4, 4)?[..]).get_u32() as u64;
        println!("opening SSTable");
        let block_meta = BlockMeta::decode_block_meta(file.read(block_meta_offset, file.size() - block_meta_offset)?.as_slice());
        let first_key = block_meta.first().unwrap().first_key.clone();
        let last_key = block_meta.last().unwrap().last_key.clone();
        Ok(Self {
            file: file,
            id: id,
            block_cache: block_cache,
            block_meta: block_meta,
            block_meta_offset: block_meta_offset as usize,
            first_key: first_key,
            last_key: last_key,
            bloom: None,
            max_ts: 0,
        })
    }

    /// Create a mock SST with only first key + last key metadata
    pub fn create_meta_only(
        id: usize,
        file_size: u64,
        first_key: KeyBytes,
        last_key: KeyBytes,
    ) -> Self {
        Self {
            file: FileObject(None, file_size),
            block_meta: vec![],
            block_meta_offset: 0,
            id,
            block_cache: None,
            first_key,
            last_key,
            bloom: None,
            max_ts: 0,
        }
    }

    /// Read a block from the disk.
    pub fn read_block(&self, block_idx: usize) -> Result<Arc<Block>> {
        // read the block from the file using the offset block_meta info
        let offset = self.block_meta[block_idx].offset;
        let offset_end = self.block_meta.get(block_idx + 1)
            .map_or(self.block_meta_offset, |x| x.offset);
        println!("read_block from offset {:?} to {:?}", offset, offset_end);
        Ok(Arc::new(Block::decode(
            &self.file.read(offset as u64,
                            (offset_end - offset) as u64)?[..])))
    }

    /// Read a block from disk, with block cache. (Day 4)
    pub fn read_block_cached(&self, block_idx: usize) -> Result<Arc<Block>> {
        let result = self.block_cache.as_ref().map_or(
            self.read_block(block_idx),
            |c|
                Ok(c.try_get_with((self.id, block_idx), || self.read_block(block_idx)).unwrap()));
        result
    }

    /// Find the block that may contain `key`.
    /// Note: You may want to make use of the `first_key` stored in `BlockMeta`.
    /// You may also assume the key-value pairs stored in each consecutive block are sorted.
    pub fn find_block_idx(&self, key: KeySlice) -> usize {
        for blk_idx in 0..self.block_meta.len() {
            let bm = &self.block_meta[blk_idx];
            println!("seek_to_key - blk_idx={:?} first_key={:?}, last_key={:?}", blk_idx, bm.first_key, bm.last_key);
            if key <= bm.last_key.as_key_slice() {
                return blk_idx
            }
        }
        self.block_meta.len()
    }

    /// Get number of data blocks.
    pub fn num_of_blocks(&self) -> usize {
        self.block_meta.len()
    }

    pub fn first_key(&self) -> &KeyBytes {
        &self.first_key
    }

    pub fn last_key(&self) -> &KeyBytes {
        &self.last_key
    }

    pub fn table_size(&self) -> u64 {
        self.file.1
    }

    pub fn sst_id(&self) -> usize {
        self.id
    }

    pub fn max_ts(&self) -> u64 {
        self.max_ts
    }
}
