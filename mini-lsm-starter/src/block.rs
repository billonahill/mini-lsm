#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

mod builder;
mod iterator;

pub use builder::BlockBuilder;
use bytes::{Buf, BufMut, Bytes};
pub use iterator::BlockIterator;
use nom::Slice;

pub(crate) const SIZEOF_U16: usize = std::mem::size_of::<u16>();

fn as_u16(bytes: &[u8]) -> u16 {
    // assert_eq!(bytes.len(), 2)
    ((bytes[0] as u16) << 8) | bytes[1] as u16
}

/// A block is the smallest unit of read and caching in LSM tree. It is a collection of sorted key-value pairs.
pub struct Block {
    pub(crate) data: Vec<u8>,
    pub(crate) offsets: Vec<u16>,
}

impl Block {
    /// Encode the internal data to the data layout illustrated in the tutorial
    /// Note: You may want to recheck if any of the expected field is missing from your output
    pub fn encode(&self) -> Bytes {
        let mut buf = self.data.clone();
        let offsets_length = self.offsets.len();
        for offset in &self.offsets {
            buf.put_u16(*offset);
        }
        buf.put_u16(offsets_length as u16);
        buf.into()
    }

    /// Decode from the data layout, transform the input `data` to a single `Block`
    pub fn decode(data: &[u8]) -> Self {
        let num_as_bytes = &data[data.len() - SIZEOF_U16..];
        let num = as_u16(num_as_bytes) as usize;
        let data_bytes_length = data.len() - SIZEOF_U16 - num * SIZEOF_U16;
        let offsets: Vec<u16> = data[data_bytes_length..data.len() - SIZEOF_U16] // leave num off
            .chunks(SIZEOF_U16)
            .map(|mut a| a.get_u16())
            .collect();

        Self {
            data: data[..data_bytes_length].to_vec(),
            offsets,
        }
    }
}
