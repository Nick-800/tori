use super::{AnnounceRequest, AnnounceResponse, Tracker, TrackerError};
use async_trait::async_trait;
use reqwest::Client;
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use url::Url;
use crate::bencode::{Bencode, BencodeDict, Metainfo};

pub struct HttpTracker {
    url: Url,
    client: Client,
}

impl HttpTracker {
    pub fn new(url: Url) -> Self {
        Self {
            url,
            client: Client::new(),
        }
    }

    fn url_encode(bytes: &[u8]) -> String {
        let mut out = String::with_capacity(bytes.len() * 3);
        for &b in bytes {
            match b {
                b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                    out.push(b as char);
                }
                _ => {
                    out.push_str(&format!("%{:02X}", b));
                }
            }
        }
        out
    }
}

#[async_trait]
impl Tracker for HttpTracker {
    async fn announce(&self, req: AnnounceRequest) -> Result<AnnounceResponse, TrackerError> {
        let mut url_string = self.url.to_string();
        
        let query_sep = if url_string.contains('?') { "&" } else { "?" };
        url_string.push_str(&format!(
            "{}info_hash={}&peer_id={}&port={}&uploaded={}&downloaded={}&left={}&compact={}",
            query_sep,
            Self::url_encode(&req.info_hash),
            Self::url_encode(&req.peer_id),
            req.port,
            req.uploaded,
            req.downloaded,
            req.left,
            if req.compact { 1 } else { 0 }
        ));

        let res = self
            .client
            .get(&url_string)
            .send()
            .await
            .map_err(|e| TrackerError::Network(e.to_string()))?;

        let bytes = res
            .bytes()
            .await
            .map_err(|e| TrackerError::Network(e.to_string()))?;

        let bencode: Bencode = bendy::serde::from_bytes(&bytes)
            .map_err(|e| TrackerError::Parse(format!("Bencode parse error: {}", e)))?;

        let dict = match bencode {
            Bencode::Dict(d) => d,
            _ => return Err(TrackerError::Parse("Expected bencode dict".to_string())),
        };

        if let Some(Bencode::String(failure)) = dict.get(&b"failure reason"[..]) {
            let msg = String::from_utf8_lossy(failure).to_string();
            return Err(TrackerError::TrackerFailure(msg));
        }

        let interval = match dict.get(&b"interval"[..]) {
            Some(Bencode::Integer(i)) => *i as u32,
            _ => 1800, // default 30 mins
        };

        let mut peer_addrs = Vec::new();
        match dict.get(&b"peers"[..]) {
            Some(Bencode::String(peers_bytes)) => {
                if peers_bytes.len() % 6 != 0 {
                    return Err(TrackerError::Parse("Invalid compact peers string".to_string()));
                }
                for chunk in peers_bytes.chunks_exact(6) {
                    let ip = Ipv4Addr::new(chunk[0], chunk[1], chunk[2], chunk[3]);
                    let port = u16::from_be_bytes([chunk[4], chunk[5]]);
                    peer_addrs.push(SocketAddr::V4(SocketAddrV4::new(ip, port)));
                }
            }
            Some(Bencode::List(_)) => {
                // Non-compact peer list not supported yet
                tracing::warn!("Non-compact peer response received, not supported");
            }
            _ => {}
        }

        Ok(AnnounceResponse {
            interval,
            peers: peer_addrs,
        })
    }
}
