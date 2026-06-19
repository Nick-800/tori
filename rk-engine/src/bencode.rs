use serde::{Deserialize, Serialize};
use sha1::{Digest, Sha1};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum MetainfoError {
    #[error("Bencode deserialization failed: {0}")]
    Bencode(#[from] bendy::serde::Error),
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TorrentInfo {
    pub name: String,
    #[serde(rename = "piece length")]
    pub piece_length: i64,
    pub pieces: bytes::Bytes,
    /// Length field for single-file mode
    pub length: Option<i64>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Metainfo {
    pub announce: String,
    pub info: TorrentInfo,
}

#[derive(Debug, Clone)]
pub struct TorrentMetadata {
    pub meta: Metainfo,
    pub info_hash: [u8; 20],
}

impl TorrentMetadata {
    /// Deserializes raw torrent file bytes and derives the definitive Info-Hash
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, MetainfoError> {
        let meta: Metainfo = bendy::serde::from_bytes(bytes)?;
        
        // Re-serialize the exact inner info dictionary to compute its SHA-1 hash
        let raw_info = bendy::serde::to_bytes(&meta.info)?;
        let mut hasher = Sha1::new();
        hasher.update(&raw_info);
        let result = hasher.finalize();
        
        let mut info_hash = [0u8; 20];
        info_hash.copy_from_slice(&result);
        
        Ok(Self { meta, info_hash })
    }
}
