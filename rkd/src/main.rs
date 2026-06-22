use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{watch, RwLock};
use tokio_stream::wrappers::WatchStream;
use tokio_stream::StreamExt;
use futures::Stream;
use std::pin::Pin;
use tokio_util::sync::CancellationToken;
use tonic::{transport::Server, Request, Response, Status};

pub mod torrent_proto {
    tonic::include_proto!("torrent");
}
use torrent_proto::torrent_service_server::{TorrentService, TorrentServiceServer};
use torrent_proto::*;

/// Represents the actual internal state of the daemon
pub struct DaemonState {
    pub active_downloads: Vec<TorrentStatus>,
}

pub struct TorrentServerImpl {
    pub state: Arc<RwLock<DaemonState>>,
    pub tx: watch::Sender<StatusUpdate>,
}

#[tonic::async_trait]
impl TorrentService for TorrentServerImpl {
    async fn add_torrent(
        &self,
        request: Request<AddTorrentRequest>,
    ) -> Result<Response<AddTorrentResponse>, Status> {
        let inner = request.into_inner();
        let mut state = self.state.write().await;
        
        // Prefer magnet URI if provided
        let info_hash = if !inner.magnet.is_empty() {
            let mut hash = "invalid".to_string();
            if let Some(query) = inner.magnet.strip_prefix("magnet:?") {
                for pair in query.split('&') {
                    if let Some(xt) = pair.strip_prefix("xt=urn:btih:") {
                        hash = xt.to_string();
                        break;
                    }
                }
            }
            hash
        } else {
            // Existing placeholder hash for non‑magnet torrents
            "a1b2c3d4e5f67890".to_string()
        };

        let new_torrent = TorrentStatus {
            name: if !inner.magnet.is_empty() { inner.magnet.clone() } else { inner.target.clone() },
            info_hash,
            progress: 0.0,
            download_speed: 0,
            upload_speed: 0,
            peer_count: 0,
        };
        
        state.active_downloads.push(new_torrent);
        
        // Notify any active stream status receivers of the updated list
        let _ = self.tx.send(StatusUpdate {
            torrents: state.active_downloads.clone(),
        });
        
        Ok(Response::new(AddTorrentResponse {
            info_hash: "a1b2c3d4e5f67890".to_string(),
            success: true,
        }))
    }

    async fn list_torrents(
        &self,
        _request: Request<ListTorrentsRequest>,
    ) -> Result<Response<ListTorrentsResponse>, Status> {
        let state = self.state.read().await;
        Ok(Response::new(ListTorrentsResponse {
            torrents: state.active_downloads.clone(),
        }))
    }

    type StreamStatusStream = Pin<Box<dyn Stream<Item = Result<StatusUpdate, Status>> + Send>>;

    async fn stream_status(
        &self,
        _request: Request<StatusRequest>,
    ) -> Result<Response<Self::StreamStatusStream>, Status> {
        let rx = self.tx.subscribe();
        let stream = Box::pin(WatchStream::new(rx).map(Ok));
        Ok(Response::new(stream))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();
    
    let addr = "[::1]:50051".parse()?;
    let shutdown_token = CancellationToken::new();
    
    let initial_status = StatusUpdate { torrents: vec![] };
    let (tx, _rx) = watch::channel(initial_status);
    
    let state = Arc::new(RwLock::new(DaemonState {
        active_downloads: Vec::new(),
    }));
    
    // Clone state and tx to run background simulator updating progress speeds
    let state_clone = Arc::clone(&state);
    let tx_clone = tx.clone();
    let token_clone = shutdown_token.clone();
    
    tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = token_clone.cancelled() => {
                    tracing::info!("State update worker received shutdown signal.");
                    break;
                }
                _ = tokio::time::sleep(Duration::from_secs(1)) => {
                    let mut lock = state_clone.write().await;
                    for t in &mut lock.active_downloads {
                        if t.progress < 1.0 {
                            t.progress = (t.progress + 0.05).min(1.0);
                            t.download_speed = 1_024 * 342;
                            t.upload_speed = 1_024 * 32;
                            t.peer_count = 14;
                        } else {
                            t.download_speed = 0;
                            t.upload_speed = 0;
                        }
                    }
                    let _ = tx_clone.send(StatusUpdate {
                        torrents: lock.active_downloads.clone(),
                    });
                }
            }
        }
    });

    let service = TorrentServerImpl { state, tx };
    
    tracing::info!("Starting Rkd gRPC Server on {}", addr);
    
    Server::builder()
        .add_service(TorrentServiceServer::new(service))
        .serve_with_shutdown(addr, async {
            let _ = tokio::signal::ctrl_c().await;
            tracing::info!("Shutdown signal intercepted. Closing server...");
            shutdown_token.cancel();
        })
        .await?;
        
    Ok(())
}
