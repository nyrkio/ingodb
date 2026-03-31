mod value;
mod iblob;
mod error;

pub use value::Value;
pub use iblob::IBlob;
pub use error::BlobError;

/// 32-byte content hash (BLAKE3)
pub type ContentHash = [u8; 32];

/// Magic bytes identifying an I-Blob on disk: "INGO"
pub const MAGIC: [u8; 4] = *b"INGO";

/// Current format version
pub const FORMAT_VERSION: u16 = 1;
