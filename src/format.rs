//! Module provides serialization/deserialization ops.

use chrono::Utc;

use crate::errors::FormatError;

pub(crate) const HEADER_SIZE: usize = 12;

/// DB entry Header. It contains the following entry metadata:
///     - timestamp
///     - key size
///     - value size
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct Header([u8; HEADER_SIZE]);

impl Header {
    /// Creates a new `Header`.
    pub fn new(timestamp: u32, key_size: u32, value_size: u32) -> Self {
        let mut buf = [0; 12];

        buf[..4].copy_from_slice(&timestamp.to_le_bytes());
        buf[4..8].copy_from_slice(&key_size.to_le_bytes());
        buf[8..].copy_from_slice(&value_size.to_le_bytes());

        Self(buf)
    }

    /// Entry timestamp.
    pub fn timestamp(&self) -> u32 {
        u32::from_le_bytes(self.0[..4].try_into().unwrap())
    }

    /// Entry key size.
    pub fn key_size(&self) -> usize {
        u32::from_le_bytes(self.0[4..8].try_into().unwrap()) as usize
    }

    /// Entry value size.
    pub fn value_size(&self) -> usize {
        u32::from_le_bytes(self.0[8..].try_into().unwrap()) as usize
    }

    /// Returns a slice to the underlying header byte representation.
    pub fn as_slice(&self) -> &[u8] {
        &self.0
    }
}

impl From<(u32, u32, u32)> for Header {
    fn from(entry_tuple: (u32, u32, u32)) -> Self {
        Self::new(entry_tuple.0, entry_tuple.1, entry_tuple.2)
    }
}

impl From<[u8; HEADER_SIZE]> for Header {
    fn from(value: [u8; HEADER_SIZE]) -> Self {
        Self(value)
    }
}

impl TryFrom<&[u8]> for Header {
    type Error = FormatError;

    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        if value.len() != 12 {
            return Err(FormatError::DeserializeError);
        }

        let mut buf = [0; 12];

        buf.copy_from_slice(value);

        Ok(Self(buf))
    }
}

impl From<Header> for [u8; HEADER_SIZE] {
    fn from(header: Header) -> Self {
        header.0
    }
}

/// Entry disk representation.
#[derive(Debug, Clone)]
pub(crate) struct DiskEntry {
    pub header: Header,
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

impl DiskEntry {
    /// Creates a new `DiskEntry`.
    pub fn new(key: impl AsRef<[u8]>, value: impl AsRef<[u8]>) -> Self {
        let timestamp: u32 = Utc::now().timestamp().try_into().unwrap();
        let key_size = key.as_ref().len() as u32;
        let value_size = value.as_ref().len() as u32;

        let header = Header::new(timestamp, key_size, value_size);
        let key = key.as_ref().to_vec();
        let value = value.as_ref().to_vec();

        Self { header, key, value }
    }
}

/// Keydir in-memory entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct KeydirEntry {
    pub file_id: u32,
    pub value_size: usize,
    pub value_pos: u64,
    pub timestamp: u32,
}

impl KeydirEntry {
    /// Creates a new `DiskEntry`.
    pub fn new(file_id: u32, value_size: usize, value_pos: u64, timestamp: u32) -> Self {
        Self {
            file_id,
            value_size,
            value_pos,
            timestamp,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::Rng;

    fn header_test(header: Header) {
        let data: [u8; HEADER_SIZE] = header.clone().into();
        let deserialized_header = Header::from(data);

        assert_eq!(header, deserialized_header);
    }

    fn random_header() -> Header {
        let mut rng = rand::rng();

        Header::new(rng.random(), rng.random(), rng.random())
    }

    #[test]
    fn it_should_serialize_header() {
        let tests = [
            Header::new(10, 10, 10),
            Header::new(0, 0, 0),
            Header::new(10000, 10000, 10000),
        ];

        for test in tests {
            header_test(test);
        }
    }

    #[test]
    fn it_should_serialize_header_random() {
        for _ in 0..100 {
            header_test(random_header())
        }
    }

    #[test]
    fn it_should_create_disk_entry() {
        let entry = DiskEntry::new(b"hello", b"world");

        assert_eq!(entry.header.key_size(), 5);
        assert_eq!(entry.header.value_size(), 5);
    }
}
