use crate::codec::{PwpCodec, PwpMessage};
use futures::{SinkExt, StreamExt};
use std::io;
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio_util::codec::Framed;

use super::{PeerCommand, PeerState};

pub struct PeerConnection {
    pub state: PeerState,
    framed: Framed<TcpStream, PwpCodec>,
    cmd_rx: mpsc::Receiver<PeerCommand>,
}

impl PeerConnection {
    pub fn new(stream: TcpStream, cmd_rx: mpsc::Receiver<PeerCommand>) -> Self {
        Self {
            state: PeerState::new(),
            framed: Framed::new(stream, PwpCodec),
            cmd_rx,
        }
    }

    pub async fn run_loop(mut self) -> io::Result<()> {
        loop {
            tokio::select! {
                // Incoming messages from the network
                Some(msg_result) = self.framed.next() => {
                    let msg = msg_result?;
                    self.handle_network_message(msg).await?;
                }
                
                // Outgoing commands from our engine
                Some(cmd) = self.cmd_rx.recv() => {
                    self.handle_engine_command(cmd).await?;
                }
                
                else => {
                    // Connection closed
                    break;
                }
            }
        }
        Ok(())
    }

    async fn handle_network_message(&mut self, msg: PwpMessage) -> io::Result<()> {
        match msg {
            PwpMessage::Choke => {
                self.state.peer_choking = true;
                tracing::debug!("Peer choked us");
            }
            PwpMessage::Unchoke => {
                self.state.peer_choking = false;
                tracing::debug!("Peer unchoked us");
            }
            PwpMessage::Interested => {
                self.state.peer_interested = true;
                tracing::debug!("Peer is interested");
            }
            PwpMessage::NotInterested => {
                self.state.peer_interested = false;
                tracing::debug!("Peer is not interested");
            }
            PwpMessage::Have(index) => {
                // Technically we should update a bitfield here, but we will
                // just log for now until Piece Manager is implemented.
                tracing::debug!("Peer has piece {}", index);
            }
            PwpMessage::Bitfield(_data) => {
                tracing::debug!("Received bitfield from peer");
                // TODO: Store in state.bitfield
            }
            PwpMessage::Piece { index, begin, block } => {
                tracing::debug!("Received piece {} block ({} bytes) at {}", index, block.len(), begin);
                // TODO: Send to Piece Manager / Disk Actor
            }
            _ => {
                // KeepAlive, Request, Cancel etc handled here later
            }
        }
        Ok(())
    }

    async fn handle_engine_command(&mut self, cmd: PeerCommand) -> io::Result<()> {
        match cmd {
            PeerCommand::SendInterested => {
                self.state.am_interested = true;
                self.framed.send(PwpMessage::Interested).await?;
            }
            PeerCommand::SendNotInterested => {
                self.state.am_interested = false;
                self.framed.send(PwpMessage::NotInterested).await?;
            }
            PeerCommand::RequestPiece { index, begin, length } => {
                if !self.state.peer_choking {
                    self.framed.send(PwpMessage::Request { index, begin, length }).await?;
                }
            }
            PeerCommand::Disconnect => {
                return Err(io::Error::new(io::ErrorKind::ConnectionAborted, "Engine requested disconnect"));
            }
        }
        Ok(())
    }
}
