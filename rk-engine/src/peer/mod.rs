pub mod connection;
pub mod handshake;

use std::net::SocketAddr;
use tokio::net::TcpStream;
use tokio::sync::mpsc;

#[derive(Debug, Clone)]
pub struct PeerState {
    pub am_choking: bool,
    pub am_interested: bool,
    pub peer_choking: bool,
    pub peer_interested: bool,
    // bitfield will go here eventually
}

impl PeerState {
    pub fn new() -> Self {
        Self {
            am_choking: true,
            am_interested: false,
            peer_choking: true,
            peer_interested: false,
        }
    }
}

impl Default for PeerState {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug)]
pub enum PeerCommand {
    SendInterested,
    SendNotInterested,
    RequestPiece {
        index: u32,
        begin: u32,
        length: u32,
    },
    Disconnect,
}

#[derive(thiserror::Error, Debug)]
pub enum PeerError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Handshake failed: {0}")]
    Handshake(String),
}

/// Connects to a peer, performs the handshake, and returns a PeerConnection
pub async fn connect(
    addr: SocketAddr,
    info_hash: [u8; 20],
    peer_id: [u8; 20],
    cmd_rx: mpsc::Receiver<PeerCommand>,
) -> Result<connection::PeerConnection, PeerError> {
    // Open TCP connection with a timeout
    let mut stream = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        TcpStream::connect(addr),
    )
    .await
    .map_err(|_| {
        PeerError::Io(std::io::Error::new(
            std::io::ErrorKind::TimedOut,
            "TCP connect timeout",
        ))
    })??;

    // Perform handshake
    let _remote_peer_id = handshake::perform_handshake(&mut stream, &info_hash, &peer_id)
        .await
        .map_err(|e| PeerError::Handshake(e.to_string()))?;

    // Create the connection object
    Ok(connection::PeerConnection::new(stream, cmd_rx))
}
