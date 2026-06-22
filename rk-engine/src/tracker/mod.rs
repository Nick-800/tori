use async_trait::async_trait;
use std::net::SocketAddr;
use thiserror::Error;
use tokio::sync::mpsc;
use url::Url;

pub mod http;
pub mod udp;

#[derive(Debug, Clone)]
pub struct AnnounceRequest {
    pub info_hash: [u8; 20],
    pub peer_id: [u8; 20],
    pub port: u16,
    pub uploaded: u64,
    pub downloaded: u64,
    pub left: u64,
    pub compact: bool,
}

#[derive(Debug)]
pub struct AnnounceResponse {
    pub interval: u32,
    pub peers: Vec<SocketAddr>,
}

#[derive(Error, Debug)]
pub enum TrackerError {
    #[error("Network error: {0}")]
    Network(String),
    #[error("Parse error: {0}")]
    Parse(String),
    #[error("Tracker returned failure: {0}")]
    TrackerFailure(String),
    #[error("Unsupported protocol: {0}")]
    UnsupportedProtocol(String),
}

#[async_trait]
pub trait Tracker: Send + Sync {
    async fn announce(&self, req: AnnounceRequest) -> Result<AnnounceResponse, TrackerError>;
}

pub struct TrackerManager {
    trackers: Vec<Url>,
}

impl TrackerManager {
    pub fn new(urls: Vec<String>) -> Self {
        let trackers = urls
            .into_iter()
            .filter_map(|s| Url::parse(&s).ok())
            .collect();
        Self { trackers }
    }

    /// Spawns concurrent requests to all valid trackers and sends discovered
    /// peer addresses to the provided `mpsc::Sender`.
    pub async fn announce_all(
        &self,
        req: AnnounceRequest,
        peer_tx: mpsc::Sender<SocketAddr>,
    ) {
        for url in &self.trackers {
            let tracker: Box<dyn Tracker> = match url.scheme() {
                "http" | "https" => Box::new(http::HttpTracker::new(url.clone())),
                "udp" => Box::new(udp::UdpTracker::new(url.clone())),
                _ => {
                    tracing::warn!("Unsupported tracker scheme: {}", url.scheme());
                    continue;
                }
            };

            let req_clone = req.clone();
            let tx_clone = peer_tx.clone();
            let url_string = url.to_string();

            tokio::spawn(async move {
                tracing::info!("Announcing to tracker: {}", url_string);
                match tracker.announce(req_clone).await {
                    Ok(resp) => {
                        tracing::info!("Received {} peers from {}", resp.peers.len(), url_string);
                        for peer in resp.peers {
                            let _ = tx_clone.send(peer).await;
                        }
                    }
                    Err(e) => {
                        tracing::warn!("Failed to announce to {}: {}", url_string, e);
                    }
                }
            });
        }
    }
}
