use crate::{BlobError, ContentHash, DocumentId, Value, FORMAT_VERSION, MAGIC};
use std::collections::BTreeMap;

/// Index entry in the I-Blob header: maps a key hash to its location in the payload.
#[derive(Debug, Clone, Copy)]
struct IndexEntry {
    /// Hash of the field key (first 8 bytes of BLAKE3)
    key_hash: u64,
    /// Byte offset into the payload section
    offset: u32,
    /// Length of the encoded value in bytes
    length: u32,
}

const INDEX_ENTRY_SIZE: usize = 16; // 8 + 4 + 4
// header: magic(4) + format_version(2) + document_id(16) + doc_version(16) + is_deleted(1) + content_hash(32) + index_count(4) = 75
const HEADER_SIZE: usize = 4 + 2 + 16 + 16 + 1 + 32 + 4;

/// An IngoDB document: a collection of named fields with stable identity.
///
/// Binary layout:
/// ```text
/// [magic: 4B "INGO"]
/// [format_version: 2B LE]
/// [document_id: 16B UUIDv7]
/// [doc_version: 16B UUIDv7]
/// [is_deleted: 1B]
/// [content_hash: 32B BLAKE3]
/// [index_count: 4B LE]
/// [index_entries: index_count * 16B]
///   each: [key_hash: 8B LE][offset: 4B LE][length: 4B LE]
/// [payload: variable]
///   encoded Values concatenated in key-sorted order
/// ```
///
/// `_id` is the stable document identity (persists across updates).
/// `_version` is server-assigned at write time (advances on each update).
/// `is_deleted` marks tombstones — the document has been deleted.
/// The content hash covers `_id` + `_version` + `is_deleted` + fields for full integrity protection.
#[derive(Debug, Clone)]
pub struct IBlob {
    /// Stable document identity (UUIDv7)
    id: DocumentId,
    /// Server-assigned write-order version (UUIDv7). Nil until server stamps it.
    version: DocumentId,
    /// Whether this is a tombstone (deleted document)
    is_deleted: bool,
    /// BLAKE3 hash of the canonical payload (for integrity/dedup)
    hash: ContentHash,
    /// Fields stored as sorted key-value pairs
    fields: BTreeMap<String, Value>,
    /// Cached serialized form (lazily populated)
    encoded: Option<Vec<u8>>,
}

impl IBlob {
    /// Create a new IBlob from key-value pairs.
    /// Auto-generates a UUIDv7 `_id`. The `_version` is left nil (server sets it at write time).
    pub fn new(fields: BTreeMap<String, Value>) -> Self {
        let mut blob = IBlob {
            id: DocumentId::new(),
            version: DocumentId::nil(),
            is_deleted: false,
            hash: [0; 32],
            fields,
            encoded: None,
        };
        blob.recompute_hash();
        blob
    }

    /// Create with a caller-supplied `_id`. The `_version` is left nil.
    pub fn with_id(id: DocumentId, fields: BTreeMap<String, Value>) -> Self {
        let mut blob = IBlob {
            id,
            version: DocumentId::nil(),
            is_deleted: false,
            hash: [0; 32],
            fields,
            encoded: None,
        };
        blob.recompute_hash();
        blob
    }

    /// Create a tombstone for the given document ID.
    /// Empty fields, but hash covers id + version + is_deleted for integrity.
    pub fn tombstone(id: DocumentId) -> Self {
        let mut blob = IBlob {
            id,
            version: DocumentId::nil(),
            is_deleted: true,
            hash: [0; 32],
            fields: BTreeMap::new(),
            encoded: None,
        };
        blob.recompute_hash();
        blob
    }

    /// Create from a list of (key, value) tuples.
    pub fn from_pairs(pairs: Vec<(impl Into<String>, Value)>) -> Self {
        let fields: BTreeMap<String, Value> = pairs
            .into_iter()
            .map(|(k, v)| (k.into(), v))
            .collect();
        Self::new(fields)
    }

    /// Stable document identity.
    pub fn id(&self) -> &DocumentId {
        &self.id
    }

    /// Server-assigned version (nil until written through the engine).
    pub fn version(&self) -> &DocumentId {
        &self.version
    }

    /// Set the version. Called by the LSM engine at write time.
    /// Recomputes the hash since version is included in the hash.
    pub fn set_version(&mut self, v: DocumentId) {
        self.version = v;
        self.encoded = None; // invalidate cached encoding
        self.recompute_hash();
    }

    /// Whether this is a tombstone (deleted document).
    pub fn is_deleted(&self) -> bool {
        self.is_deleted
    }

    /// Content hash of this document's payload (for integrity/dedup).
    pub fn hash(&self) -> &ContentHash {
        &self.hash
    }

    /// Access the fields.
    pub fn fields(&self) -> &BTreeMap<String, Value> {
        &self.fields
    }

    /// Get a field by name.
    pub fn get(&self, key: &str) -> Option<&Value> {
        self.fields.get(key)
    }

    /// Number of fields.
    pub fn field_count(&self) -> usize {
        self.fields.len()
    }

    /// Compute the key hash used in the index (first 8 bytes of BLAKE3 of the key).
    fn key_hash(key: &str) -> u64 {
        let h = blake3::hash(key.as_bytes());
        u64::from_le_bytes(h.as_bytes()[..8].try_into().unwrap())
    }

    fn recompute_hash(&mut self) {
        self.hash = *blake3::hash(&self.encode_hashable()).as_bytes();
    }

    /// Encode the hashable content: _id + _version + is_deleted + fields.
    /// This is what the BLAKE3 content hash covers — all identity and payload bytes.
    fn encode_hashable(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend_from_slice(self.id.as_bytes());
        buf.extend_from_slice(self.version.as_bytes());
        buf.push(if self.is_deleted { 1 } else { 0 });
        for (key, value) in &self.fields {
            buf.extend_from_slice(&(key.len() as u32).to_le_bytes());
            buf.extend_from_slice(key.as_bytes());
            value.encode(&mut buf);
        }
        buf
    }

    /// Serialize this IBlob to its binary format.
    pub fn encode(&self) -> Vec<u8> {
        if let Some(ref cached) = self.encoded {
            return cached.clone();
        }

        // Encode each field
        let mut value_buffers: Vec<(String, u64, Vec<u8>)> = Vec::with_capacity(self.fields.len());
        for (key, value) in &self.fields {
            let kh = Self::key_hash(key);
            let mut vbuf = Vec::new();
            vbuf.extend_from_slice(&(key.len() as u32).to_le_bytes());
            vbuf.extend_from_slice(key.as_bytes());
            value.encode(&mut vbuf);
            value_buffers.push((key.clone(), kh, vbuf));
        }

        // Build the index entries and payload
        let index_count = value_buffers.len() as u32;
        let mut payload = Vec::new();
        let mut index_entries: Vec<IndexEntry> = Vec::with_capacity(value_buffers.len());

        for (_key, kh, vbuf) in &value_buffers {
            let offset = payload.len() as u32;
            let length = vbuf.len() as u32;
            index_entries.push(IndexEntry {
                key_hash: *kh,
                offset,
                length,
            });
            payload.extend_from_slice(vbuf);
        }

        // Sort index entries by key_hash for binary search during reads
        index_entries.sort_by_key(|e| e.key_hash);

        // Assemble the full blob
        let total_size =
            HEADER_SIZE + (index_entries.len() * INDEX_ENTRY_SIZE) + payload.len();
        let mut buf = Vec::with_capacity(total_size);

        // Header
        buf.extend_from_slice(&MAGIC);
        buf.extend_from_slice(&FORMAT_VERSION.to_le_bytes());
        buf.extend_from_slice(self.id.as_bytes());
        buf.extend_from_slice(self.version.as_bytes());
        buf.push(if self.is_deleted { 1 } else { 0 });
        buf.extend_from_slice(&self.hash);
        buf.extend_from_slice(&index_count.to_le_bytes());

        // Index entries
        for entry in &index_entries {
            buf.extend_from_slice(&entry.key_hash.to_le_bytes());
            buf.extend_from_slice(&entry.offset.to_le_bytes());
            buf.extend_from_slice(&entry.length.to_le_bytes());
        }

        // Payload
        buf.extend_from_slice(&payload);

        buf
    }

    /// Decode an IBlob from its binary format.
    pub fn decode(buf: &[u8]) -> Result<Self, BlobError> {
        if buf.len() < HEADER_SIZE {
            return Err(BlobError::BufferTooShort {
                need: HEADER_SIZE,
                have: buf.len(),
            });
        }

        // Magic
        if buf[0..4] != MAGIC {
            return Err(BlobError::InvalidMagic);
        }

        // Format version
        let version = u16::from_le_bytes(buf[4..6].try_into().unwrap());
        if version != FORMAT_VERSION {
            return Err(BlobError::UnsupportedVersion(version));
        }

        // Document ID (offset 6..22)
        let mut id_bytes = [0u8; 16];
        id_bytes.copy_from_slice(&buf[6..22]);
        let id = DocumentId::from_bytes(id_bytes);

        // Document version (offset 22..38)
        let mut ver_bytes = [0u8; 16];
        ver_bytes.copy_from_slice(&buf[22..38]);
        let doc_version = DocumentId::from_bytes(ver_bytes);

        // is_deleted (offset 38)
        let is_deleted = buf[38] != 0;

        // Content hash (offset 39..71)
        let mut stored_hash = [0u8; 32];
        stored_hash.copy_from_slice(&buf[39..71]);

        // Index count (offset 71..75)
        let index_count =
            u32::from_le_bytes(buf[71..75].try_into().unwrap()) as usize;

        let index_end = HEADER_SIZE + index_count * INDEX_ENTRY_SIZE;
        if buf.len() < index_end {
            return Err(BlobError::BufferTooShort {
                need: index_end,
                have: buf.len(),
            });
        }

        // Read index entries
        let mut index_entries = Vec::with_capacity(index_count);
        for i in 0..index_count {
            let base = HEADER_SIZE + i * INDEX_ENTRY_SIZE;
            let key_hash = u64::from_le_bytes(buf[base..base + 8].try_into().unwrap());
            let offset = u32::from_le_bytes(buf[base + 8..base + 12].try_into().unwrap());
            let length = u32::from_le_bytes(buf[base + 12..base + 16].try_into().unwrap());
            index_entries.push(IndexEntry {
                key_hash,
                offset,
                length,
            });
        }

        // Payload starts after the index
        let payload = &buf[index_end..];

        // Decode fields from payload
        let mut fields = BTreeMap::new();
        for entry in &index_entries {
            let start = entry.offset as usize;
            let end = start + entry.length as usize;
            if end > payload.len() {
                return Err(BlobError::BufferTooShort {
                    need: index_end + end,
                    have: buf.len(),
                });
            }
            let field_data = &payload[start..end];

            // Decode key
            if field_data.len() < 4 {
                return Err(BlobError::BufferTooShort {
                    need: 4,
                    have: field_data.len(),
                });
            }
            let key_len =
                u32::from_le_bytes(field_data[0..4].try_into().unwrap()) as usize;
            if field_data.len() < 4 + key_len {
                return Err(BlobError::BufferTooShort {
                    need: 4 + key_len,
                    have: field_data.len(),
                });
            }
            let key = std::str::from_utf8(&field_data[4..4 + key_len])?.to_string();

            // Decode value
            let (value, _consumed) = Value::decode(field_data, 4 + key_len)?;
            fields.insert(key, value);
        }

        let blob = IBlob {
            id,
            version: doc_version,
            is_deleted,
            hash: stored_hash,
            fields,
            encoded: None,
        };

        // Verify hash — covers id + version + is_deleted + fields
        let expected_hash = *blake3::hash(&blob.encode_hashable()).as_bytes();
        if expected_hash != stored_hash {
            return Err(BlobError::HashMismatch {
                expected: hex(&expected_hash),
                actual: hex(&stored_hash),
            });
        }

        Ok(blob)
    }

    /// Zero-copy field extraction: read a single field from a serialized blob
    /// without decoding the entire document.
    pub fn extract_field(buf: &[u8], field_name: &str) -> Result<Value, BlobError> {
        if buf.len() < HEADER_SIZE {
            return Err(BlobError::BufferTooShort {
                need: HEADER_SIZE,
                have: buf.len(),
            });
        }

        let index_count =
            u32::from_le_bytes(buf[71..75].try_into().unwrap()) as usize;
        let index_end = HEADER_SIZE + index_count * INDEX_ENTRY_SIZE;

        let target_hash = Self::key_hash(field_name);

        // Binary search the index
        let mut lo = 0usize;
        let mut hi = index_count;
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            let base = HEADER_SIZE + mid * INDEX_ENTRY_SIZE;
            let kh = u64::from_le_bytes(buf[base..base + 8].try_into().unwrap());
            if kh < target_hash {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }

        // Check for match (may need to scan duplicates if key_hash collides)
        while lo < index_count {
            let base = HEADER_SIZE + lo * INDEX_ENTRY_SIZE;
            let kh = u64::from_le_bytes(buf[base..base + 8].try_into().unwrap());
            if kh != target_hash {
                break;
            }
            let offset =
                u32::from_le_bytes(buf[base + 8..base + 12].try_into().unwrap()) as usize;
            let length =
                u32::from_le_bytes(buf[base + 12..base + 16].try_into().unwrap()) as usize;

            let payload_start = index_end + offset;
            let field_data = &buf[payload_start..payload_start + length];

            // Check actual key
            let key_len =
                u32::from_le_bytes(field_data[0..4].try_into().unwrap()) as usize;
            let key = std::str::from_utf8(&field_data[4..4 + key_len])?;

            if key == field_name {
                let (value, _) = Value::decode(field_data, 4 + key_len)?;
                return Ok(value);
            }
            lo += 1;
        }

        Err(BlobError::FieldNotFound(target_hash))
    }
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

impl PartialEq for IBlob {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl Eq for IBlob {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Value;

    fn sample_blob() -> IBlob {
        IBlob::from_pairs(vec![
            ("name", Value::String("Henrik".into())),
            ("age", Value::U64(42)),
            ("active", Value::Bool(true)),
        ])
    }

    #[test]
    fn test_encode_decode_roundtrip() {
        let blob = sample_blob();
        let encoded = blob.encode();
        let decoded = IBlob::decode(&encoded).unwrap();

        assert_eq!(blob.id(), decoded.id());
        assert_eq!(blob.hash(), decoded.hash());
        assert_eq!(blob.fields(), decoded.fields());
        assert!(!decoded.is_deleted());
    }

    #[test]
    fn test_same_content_different_hash() {
        // Same fields but different _ids produce different hashes
        // (hash now covers _id + _version + fields)
        let blob1 = IBlob::from_pairs(vec![
            ("z", Value::I64(1)),
            ("a", Value::I64(2)),
        ]);
        let blob2 = IBlob::from_pairs(vec![
            ("a", Value::I64(2)),
            ("z", Value::I64(1)),
        ]);
        assert_ne!(blob1.hash(), blob2.hash(), "different _ids -> different hashes");
        assert_ne!(blob1.id(), blob2.id(), "different docs -> different ids");
    }

    #[test]
    fn test_same_id_same_fields_same_hash() {
        // Same _id and same fields produce the same hash
        let id = DocumentId::from_bytes([0x42; 16]);
        let blob1 = IBlob::with_id(id, [("x".into(), Value::I64(1))].into());
        let blob2 = IBlob::with_id(id, [("x".into(), Value::I64(1))].into());
        assert_eq!(blob1.hash(), blob2.hash());
    }

    #[test]
    fn test_different_content_different_hash() {
        let blob1 = IBlob::from_pairs(vec![("x", Value::I64(1))]);
        let blob2 = IBlob::from_pairs(vec![("x", Value::I64(2))]);
        assert_ne!(blob1.hash(), blob2.hash());
    }

    #[test]
    fn test_explicit_id() {
        let id = DocumentId::from_bytes([0x42; 16]);
        let fields: BTreeMap<String, Value> = [("k".into(), Value::U64(1))].into();
        let blob = IBlob::with_id(id, fields);
        assert_eq!(*blob.id(), id);

        // Roundtrip preserves the id
        let encoded = blob.encode();
        let decoded = IBlob::decode(&encoded).unwrap();
        assert_eq!(*decoded.id(), id);
    }

    #[test]
    fn test_version_roundtrip() {
        let mut blob = sample_blob();
        assert!(blob.version().is_nil(), "version starts nil");

        let ver = DocumentId::new();
        blob.set_version(ver);
        assert_eq!(*blob.version(), ver);

        let encoded = blob.encode();
        let decoded = IBlob::decode(&encoded).unwrap();
        assert_eq!(*decoded.version(), ver);
    }

    #[test]
    fn test_tombstone_roundtrip() {
        let id = DocumentId::new();
        let mut tomb = IBlob::tombstone(id);
        assert!(tomb.is_deleted());
        assert_eq!(tomb.field_count(), 0);
        assert_ne!(*tomb.hash(), [0u8; 32], "tombstone should have a real hash");

        // Stamp a version like the engine would — hash changes
        let hash_before = *tomb.hash();
        tomb.set_version(DocumentId::new());
        assert_ne!(*tomb.hash(), hash_before, "hash should change with version");

        let encoded = tomb.encode();
        let decoded = IBlob::decode(&encoded).unwrap();
        assert_eq!(decoded.id(), &id);
        assert!(decoded.is_deleted());
        assert_eq!(decoded.field_count(), 0);
        assert_eq!(decoded.version(), tomb.version());
        assert_eq!(decoded.hash(), tomb.hash());
    }

    #[test]
    fn test_extract_field() {
        let blob = sample_blob();
        let encoded = blob.encode();

        let name = IBlob::extract_field(&encoded, "name").unwrap();
        assert_eq!(name, Value::String("Henrik".into()));

        let age = IBlob::extract_field(&encoded, "age").unwrap();
        assert_eq!(age, Value::U64(42));

        assert!(IBlob::extract_field(&encoded, "nonexistent").is_err());
    }

    #[test]
    fn test_nested_document_blob() {
        let blob = IBlob::from_pairs(vec![
            (
                "user",
                Value::Document(vec![
                    ("name".into(), Value::String("Henrik".into())),
                    ("email".into(), Value::String("henrik@example.com".into())),
                ]),
            ),
            (
                "refs",
                Value::Array(vec![
                    Value::Ref(DocumentId::from_bytes([0xAA; 16])),
                    Value::Ref(DocumentId::from_bytes([0xBB; 16])),
                ]),
            ),
        ]);

        let encoded = blob.encode();
        let decoded = IBlob::decode(&encoded).unwrap();
        assert_eq!(blob.hash(), decoded.hash());
        assert_eq!(blob.fields(), decoded.fields());
    }

    #[test]
    fn test_empty_blob() {
        let blob = IBlob::new(BTreeMap::new());
        let encoded = blob.encode();
        let decoded = IBlob::decode(&encoded).unwrap();
        assert_eq!(decoded.field_count(), 0);
        assert_eq!(blob.hash(), decoded.hash());
    }

    #[test]
    fn test_invalid_magic() {
        let mut encoded = sample_blob().encode();
        encoded[0] = b'X';
        assert!(matches!(IBlob::decode(&encoded), Err(BlobError::InvalidMagic)));
    }
}
