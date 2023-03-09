use anyhow::Result;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum StorageIteratorError {
    #[error("unknown iterator error")]
    Unknown,
}

pub trait StorageIterator {
    /// Get the current key.
    fn key(&self) -> &[u8];

    /// Get the current value.
    fn value(&self) -> &[u8];

    /// Check if the current iterator is valid.
    fn is_valid(&self) -> bool;

    /// Move to the next position.
    fn next(&mut self) -> Result<()>;
}
