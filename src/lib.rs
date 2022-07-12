use keydir::HashmapKeydir;
use storage::DiskStorage;

pub mod errors;
mod format;
mod keydir;
pub mod storage;

pub type RumDb = DiskStorage<HashmapKeydir>;
