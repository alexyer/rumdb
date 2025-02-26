//! Keydir implementation.
//!
//! Keydir is an in-memory structure that maps all keys to their
//! corresponding locations on disk.

use std::collections::HashMap;

use crate::format::KeydirEntry;

pub trait Keydir {
    /// Returns a reference to the corresponding entry.
    fn get(&self, k: impl AsRef<[u8]>) -> Option<&KeydirEntry>;

    /// Puts a key and entry into the Keydir.
    ///
    /// If keydir did not have this key present, None is returned.
    /// If keydir did have this key present, the value is updated, and the old value is returned.
    fn put(&mut self, k: Vec<u8>, v: KeydirEntry) -> Option<KeydirEntry>;

    /// Removes an entry from the Keydir.
    fn remove(&mut self, k: impl AsRef<[u8]>);

    /// Iterates over all (key, entry) pairs in arbitrary order.
    fn iter(&self) -> impl Iterator<Item = (impl AsRef<[u8]>, &KeydirEntry)>;
}

pub trait KeydirDefault: Default {}

/// Keydir represented as a hashmap.
#[derive(Default, Debug)]
pub struct HashmapKeydir {
    mapping: HashMap<Vec<u8>, KeydirEntry>,
}

impl Keydir for HashmapKeydir {
    fn get(&self, key: impl AsRef<[u8]>) -> Option<&KeydirEntry> {
        self.mapping.get(key.as_ref())
    }

    fn put(&mut self, k: Vec<u8>, v: KeydirEntry) -> Option<KeydirEntry> {
        self.mapping.insert(k, v)
    }

    fn remove(&mut self, k: impl AsRef<[u8]>) {
        self.mapping.remove(k.as_ref());
    }

    fn iter(&self) -> impl Iterator<Item = (impl AsRef<[u8]>, &KeydirEntry)> {
        self.mapping.iter()
    }
}

impl KeydirDefault for HashmapKeydir {}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_keydir(mut keydir: impl Keydir) {
        assert_eq!(keydir.get(b"hello"), None);

        let entry = KeydirEntry::new(0, 1, 2, 3);

        keydir.put(b"hello".to_vec(), entry.clone());

        assert_eq!(keydir.get(b"hello"), Some(&entry));
    }

    #[test]
    fn hashmap_keydir_should_implement_keydir() {
        test_keydir(HashmapKeydir::default());
    }
}
