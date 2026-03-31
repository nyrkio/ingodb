use thiserror::Error;

#[derive(Debug, Error)]
pub enum SSTableError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("invalid SSTable magic")]
    InvalidMagic,

    #[error("unsupported SSTable version: {0}")]
    UnsupportedVersion(u16),

    #[error("block CRC mismatch at offset {offset}")]
    CrcMismatch { offset: u64 },

    #[error("blob decode error: {0}")]
    BlobDecode(#[from] ingodb_blob::BlobError),

    #[error("SSTable is empty")]
    Empty,

    #[error("data corrupted: {0}")]
    Corrupted(String),
}
