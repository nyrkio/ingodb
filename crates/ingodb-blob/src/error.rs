use thiserror::Error;

#[derive(Debug, Error)]
pub enum BlobError {
    #[error("invalid magic bytes")]
    InvalidMagic,

    #[error("unsupported format version: {0}")]
    UnsupportedVersion(u16),

    #[error("buffer too short: need {need} bytes, have {have}")]
    BufferTooShort { need: usize, have: usize },

    #[error("hash mismatch: expected {expected}, got {actual}")]
    HashMismatch { expected: String, actual: String },

    #[error("unknown value type tag: {0}")]
    UnknownValueType(u8),

    #[error("field not found: {0:016x}")]
    FieldNotFound(u64),

    #[error("utf-8 error: {0}")]
    Utf8(#[from] std::str::Utf8Error),
}
