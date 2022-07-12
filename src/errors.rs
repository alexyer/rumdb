//! Errors module.

use std::io;

use thiserror::Error;

#[derive(Debug, Error)]
pub enum FormatError {
    #[error("invalid bytes, cannot deserialize entry")]
    DeserializeError,
}

#[derive(Debug, Error)]
pub enum StorageError {
    #[error("invalid path")]
    IoError(#[from] io::Error),

    #[error("db is already locked")]
    AlreadyLocked,
}
