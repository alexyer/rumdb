use keydir::HashmapKeydir;
use storage::DiskStorage;

pub mod errors;
mod format;
mod keydir;
pub mod storage;

pub type RumDb = DiskStorage<HashmapKeydir>;

/// Database options.
#[derive(Debug)]
pub struct DbOptions {
    /// Maximum log file size in bytes.
    max_log_file_size: usize,
}

impl Default for DbOptions {
    fn default() -> Self {
        Self {
            max_log_file_size: 100 * 1024 * 1024, // 100 MB
        }
    }
}

impl DbOptions {
    pub fn max_log_file_size(mut self, value: usize) -> Self {
        self.max_log_file_size = value;
        self
    }
}
