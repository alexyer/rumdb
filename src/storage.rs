//! RumDB storage.

use std::{
    collections::BTreeMap,
    fs::{self, File, OpenOptions},
    io::{self, Read, Seek, SeekFrom, Write},
    os::unix::prelude::FileExt,
    path::{Path, PathBuf},
};

use crate::{
    errors::StorageError,
    format::{DiskEntry, Header, KeydirEntry, HEADER_SIZE},
    keydir::{Keydir, KeydirDefault},
    DbOptions,
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
    /// Mapping between file id and actual file.
    log_files: BTreeMap<u32, File>,

    _lock: Lockfile,

    path: PathBuf,

    opts: DbOptions,
}

impl<K> DiskStorage<K>
where
    K: Keydir + KeydirDefault,
{
    /// Opens or creates a new storage at the `path` directory.
    pub fn open(path: impl AsRef<Path>, opts: DbOptions) -> Result<Self, StorageError> {
        Self::_open(path, opts)
    }

    /// Opens or creates a new storage at the `path` directory with default options.
    pub fn open_default(path: impl AsRef<Path>) -> Result<Self, StorageError> {
        Self::_open(path, DbOptions::default())
    }

    pub fn _open(path: impl AsRef<Path>, opts: DbOptions) -> Result<Self, StorageError> {
        let path = path.as_ref();

        fs::create_dir_all(path)?;
        let lock = Lockfile::lock(path.join("LOCK")).or(Err(StorageError::AlreadyLocked))?;

        log::info!("🏗  Building keydir...");

        let (keydir, log_files) = Self::build_keydir(path)?;

        log::info!("🏗  Keydir has been built successfully");

        Ok(Self {
            path: path.to_path_buf(),
            keydir,
            log_files,
            _lock: lock,
            opts,
        })
    }

    fn build_keydir(path: &Path) -> Result<(K, BTreeMap<u32, File>), io::Error> {
        let mut file_opts = OpenOptions::new();
        file_opts.read(true).write(true).create(true);

        let mut log_files = BTreeMap::new();

        fs::read_dir(path)?
            .filter_map(Result::ok)
            .filter(|f| f.path().extension().unwrap_or_default() == "log")
            .for_each(|f| {
                if let Some(Some(file_id)) = f.file_name().to_str().map(|f| f.split(".").next()) {
                    if let Ok(file_id) = file_id.parse::<u32>() {
                        let file = file_opts.open(f.path()).expect("log file");
                        log_files.insert(file_id, file);
                    }
                }
            });

        let mut keydir = K::default();

        for (file_id, log) in log_files.iter_mut() {
            Self::ingest_log(&mut keydir, *file_id, log)?;
        }

        if log_files.is_empty() {
            let file = file_opts
                .open(path.join(Self::format_log_file_name(0)))
                .expect("log file");
            log_files.insert(0, file);
        }

        Ok((keydir, log_files))
    }

    fn ingest_log(keydir: &mut K, file_id: u32, log: &mut File) -> Result<(), io::Error> {
        log::info!("💾 Ingesting: {}", Self::format_log_file_name(file_id));

        let mut buf = [0; HEADER_SIZE];

        loop {
            if log.read(&mut buf)? == 0 {
                break;
            }

            let header = Header::from(buf);

            let key_size = header.key_size();
            let value_size = header.value_size();

            let mut key = vec![0; key_size];
            log.read_exact(&mut key)?;

            let value_pos = log.stream_position()?;

            log.seek(SeekFrom::Current(value_size.try_into().unwrap()))?;

            let timestamp = header.timestamp();

            let keydir_entry = KeydirEntry::new(file_id, value_size, value_pos, timestamp);

            if value_size > 0 {
                keydir.put(key, keydir_entry);
            } else {
                keydir.remove(&key);
            }
        }

        Ok(())
    }

    fn rotate_log(&mut self, k_size: usize, v_size: usize) -> Result<(), io::Error> {
        let mut active_file_entry = self.log_files.last_entry().unwrap();
        let active_file_id = *active_file_entry.key();
        let active_file = active_file_entry.get_mut();

        let estimated_entry_size = k_size + v_size + HEADER_SIZE;

        let current_file_size = active_file.stream_position()? as usize;

        if current_file_size + estimated_entry_size > self.opts.max_log_file_size {
            active_file.flush()?;

            let mut file_opts = OpenOptions::new();
            file_opts.read(true).write(true).create(true);

            let new_active_file_id = active_file_id + 1;
            let new_active_file = file_opts.open(
                self.path
                    .join(Self::format_log_file_name(new_active_file_id)),
            )?;

            self.log_files.insert(new_active_file_id, new_active_file);
        }

        Ok(())
    }

    fn format_log_file_name(file_id: u32) -> String {
        format!("{}.rumdb.log", file_id)
    }
}

impl<K> Storage for DiskStorage<K>
where
    K: Keydir + KeydirDefault,
{
    fn get(&self, k: &[u8]) -> Result<Option<Vec<u8>>, StorageError> {
        let res = match self.keydir.get(k) {
            Some(keydir_entry) => {
                let file_id = keydir_entry.file_id;
                let mut buf = vec![0; keydir_entry.value_size as usize];

                let file = self
                    .log_files
                    .get(&file_id)
                    .ok_or(StorageError::UnknownLogFile(file_id))?;

                file.read_exact_at(&mut buf, keydir_entry.value_pos)?;

                Some(buf)
            }
            None => None,
        };

        Ok(res)
    }

    fn put(&mut self, k: Vec<u8>, v: Vec<u8>) -> Result<(), StorageError> {
        self.rotate_log(k.len(), v.len())?;

        let disk_entry = DiskEntry::new(&k, v);

        let mut active_file_entry = self.log_files.last_entry().unwrap();

        let active_file_id = *active_file_entry.key();
        let active_file = active_file_entry.get_mut();

        active_file.write(disk_entry.header.as_slice())?;
        active_file.write(disk_entry.key.as_slice())?;
        active_file.write(disk_entry.value.as_slice())?;

        let pos = active_file.stream_position()?;
        let value_size = disk_entry.header.value_size();
        let value_pos = pos - value_size as u64;

        let timestamp = disk_entry.header.timestamp();

        let keydir_entry = KeydirEntry::new(active_file_id, value_size, value_pos, timestamp);

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
        let mut db: DiskStorage<HashmapKeydir> = DiskStorage::open_default(dir.path()).unwrap();

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
            let mut db: DiskStorage<HashmapKeydir> = DiskStorage::open_default(dir.path()).unwrap();
            db.put(b"persistence".to_vec(), b"check".to_vec()).unwrap();
            db.put(b"removed".to_vec(), b"entry".to_vec()).unwrap();
            db.remove(b"removed").unwrap();
        }

        {
            let db: DiskStorage<HashmapKeydir> = DiskStorage::open_default(dir.path()).unwrap();

            let res = db.get(b"persistence").unwrap();
            assert_eq!(res, Some(b"check".to_vec()));

            let res = db.get(b"removed").unwrap();
            assert_eq!(res, None);
        }
    }

    #[test]
    fn disk_storage_should_rotate_logs() {
        const VERSION: u8 = 3;
        let dir = tempdir::TempDir::new("disk-storage-test.db").unwrap();

        {
            let mut db: DiskStorage<HashmapKeydir> =
                DiskStorage::open(dir.path(), DbOptions::default().max_log_file_size(50)).unwrap();

            for i in 0..=VERSION {
                db.put(b"version".to_vec(), vec![i]).unwrap();
            }
        }

        assert!(
            dir.path().join("1.rumdb.log").exists(),
            "log file has not been rotated"
        );

        {
            let db: DiskStorage<HashmapKeydir> =
                DiskStorage::open(dir.path(), DbOptions::default().max_log_file_size(50)).unwrap();

            let res = db.get(b"version").unwrap();
            assert_eq!(res, Some(vec![VERSION]));
        }
    }
}
