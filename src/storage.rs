//! RumDB storage.

use std::{
    fs::{self, File, OpenOptions},
    io::{self, Read, Seek, SeekFrom, Write},
    os::unix::prelude::FileExt,
    path::{Path, PathBuf},
};

use crate::{
    errors::StorageError,
    format::{DiskEntry, Header, KeydirEntry, HEADER_SIZE},
    keydir::{Keydir, KeydirDefault},
};

/// Storge trait.
pub trait Storage {
    /// Get an entry from the storage.
    fn get(&self, k: &[u8]) -> Result<Option<Vec<u8>>, StorageError>;

    /// Put an entry into the storage.
    fn put(&mut self, k: Vec<u8>, v: Vec<u8>) -> Result<(), StorageError>;

    /// Remove an entry from the storage.
    fn remove(&mut self, k: &[u8]) -> Result<(), StorageError>;
}

/// Disk storage.
#[derive(Debug)]
pub struct DiskStorage<K>
where
    K: Keydir + Default,
{
    keydir: K,

    _lock: Lockfile,

    file: File,
    file_id: u32,
}

impl<K> DiskStorage<K>
where
    K: Keydir + KeydirDefault,
{
    /// Creates a new `DiskStorage`.
    pub fn new(path: impl AsRef<Path>) -> Result<Self, StorageError> {
        let path = path.as_ref();

        fs::create_dir_all(path).map_err(StorageError::from)?;
        let lock = Lockfile::lock(path.join("LOCK")).or(Err(StorageError::AlreadyLocked))?;

        log::info!("🏗  Building keydir...");

        let (file_id, file, keydir) = Self::build_keydir(path).map_err(StorageError::from)?;

        log::info!("🏗  Keydir has been build successfully");

        Ok(Self {
            keydir,
            _lock: lock,
            file,
            file_id,
        })
    }

    fn build_keydir(path: &Path) -> Result<(u32, File, K), io::Error> {
        let mut keydir = K::default();
        let mut file_opts = OpenOptions::new();
        file_opts.read(true).write(true).create(true);

        let mut file = file_opts.open(path.join("rumdb.log.0"))?;

        let file_id = 0;

        let mut buf = [0; HEADER_SIZE];

        loop {
            if file.read(&mut buf)? == 0 {
                break;
            }

            let header = Header::from(buf);

            let key_size = header.key_size();
            let value_size = header.value_size();

            let mut key = vec![0; key_size];
            file.read_exact(&mut key)?;

            let value_pos = file.stream_position()?;

            file.seek(SeekFrom::Current(value_size.try_into().unwrap()))?;

            let timestamp = header.timestamp();

            let keydir_entry = KeydirEntry::new(file_id, value_size, value_pos, timestamp);

            if value_size > 0 {
                keydir.put(key, keydir_entry);
            } else {
                keydir.remove(&key);
            }
        }

        Ok((file_id, file, keydir))
    }
}

impl<K> Storage for DiskStorage<K>
where
    K: Keydir + KeydirDefault,
{
    fn get(&self, k: &[u8]) -> Result<Option<Vec<u8>>, StorageError> {
        let res = match self.keydir.get(k) {
            Some(keydir_entry) => {
                let mut buf = vec![0; keydir_entry.value_size as usize];

                self.file
                    .read_exact_at(&mut buf, keydir_entry.value_pos)
                    .map_err(StorageError::from)?;

                Some(buf)
            }
            None => None,
        };

        Ok(res)
    }

    fn put(&mut self, k: Vec<u8>, v: Vec<u8>) -> Result<(), StorageError> {
        self.file
            .seek(SeekFrom::End(0))
            .map_err(StorageError::from)?;

        let disk_entry = DiskEntry::new(&k, v);

        self.file
            .write(disk_entry.header.as_slice())
            .map_err(StorageError::from)?;

        self.file
            .write(disk_entry.key.as_slice())
            .map_err(StorageError::from)?;

        self.file
            .write(disk_entry.value.as_slice())
            .map_err(StorageError::from)?;

        let pos = self.file.stream_position().map_err(StorageError::from)?;
        let value_size = disk_entry.header.value_size();
        let value_pos = pos - value_size as u64;

        let timestamp = disk_entry.header.timestamp();

        let keydir_entry = KeydirEntry::new(self.file_id, value_size, value_pos, timestamp);

        self.keydir.put(k, keydir_entry);

        Ok(())
    }

    fn remove(&mut self, k: &[u8]) -> Result<(), StorageError> {
        if self.keydir.get(k).is_some() {
            self.put(k.to_vec(), Vec::new())?;
        }

        self.keydir.remove(k);

        Ok(())
    }
}

/// A simple lockfile for `DiskStorage`.
#[derive(Debug)]
struct Lockfile {
    handle: Option<File>,
    path: PathBuf,
}

impl Lockfile {
    /// Creates a lock at the provided `path`. Fails if lock is already exists.
    fn lock(path: impl AsRef<Path>) -> Result<Self, io::Error> {
        let path = path.as_ref();

        let dir_path = path.parent().expect("lock file must have a parent");
        fs::create_dir_all(dir_path)?;

        let mut lockfile_opts = OpenOptions::new();
        lockfile_opts.read(true).write(true).create_new(true);

        let lockfile = lockfile_opts.open(path)?;

        Ok(Self {
            handle: Some(lockfile),
            path: path.to_path_buf(),
        })
    }
}

impl Drop for Lockfile {
    fn drop(&mut self) {
        self.handle.take();
        fs::remove_file(&self.path).expect("lock already dropped.");
    }
}

#[cfg(test)]
mod tests {
    use crate::keydir::HashmapKeydir;

    use super::*;

    #[test]
    fn disk_storage_should_get_put() {
        let dir = tempdir::TempDir::new("disk-storage-test.db").unwrap();
        let mut db: DiskStorage<HashmapKeydir> = DiskStorage::new(dir.path()).unwrap();

        let res = db.get(b"hello").unwrap();
        assert_eq!(res, None);

        db.put(b"hello".to_vec(), b"world".to_vec()).unwrap();

        let res = db.get(b"hello").unwrap();
        assert_eq!(res, Some(b"world".to_vec()));

        db.put(b"hello".to_vec(), b"underworld".to_vec()).unwrap();

        let res = db.get(b"hello").unwrap();
        assert_eq!(res, Some(b"underworld".to_vec()));

        db.remove(b"hello").unwrap();

        let res = db.get(b"hello").unwrap();
        assert_eq!(res, None);
    }

    #[test]
    fn disk_storage_should_persist() {
        let dir = tempdir::TempDir::new("disk-storage-test.db").unwrap();

        {
            let mut db: DiskStorage<HashmapKeydir> = DiskStorage::new(dir.path()).unwrap();
            db.put(b"persistence".to_vec(), b"check".to_vec()).unwrap();
            db.put(b"removed".to_vec(), b"entry".to_vec()).unwrap();
            db.remove(b"removed").unwrap();
        }

        {
            let db: DiskStorage<HashmapKeydir> = DiskStorage::new(dir.path()).unwrap();

            let res = db.get(b"persistence").unwrap();
            assert_eq!(res, Some(b"check".to_vec()));

            let res = db.get(b"removed").unwrap();
            assert_eq!(res, None);
        }
    }
}
