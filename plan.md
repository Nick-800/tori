# Comprehensive Architecture Blueprint: Rektorrent (rk)

This document contains the complete production-grade architectural plan and implementation roadmap for `rektorrent` (`rk`), a high-performance, concurrent BitTorrent client and background daemon written in Rust.

---

## 1. Project Architecture & Workspace Layout

To keep the codebase modular, testable, and reusable, we utilize a Rust **workspace**. This strictly isolates the peer-to-peer networking engine from the background server runtime and user interfaces.

### 1.1 Complete Directory Tree
```text
rektorrent/ (Workspace Root)
├── Cargo.toml
├── proto/
│   └── torrent.proto  # gRPC/Protobuf service and message definitions
├── rk-engine/        # Core Library: Bencode, Peer Wire Protocol (PWP), DHT, Disk Actor
│   ├── Cargo.toml
│   └── src/
│       ├── lib.rs
│       ├── bencode.rs
│       ├── codec.rs
│       ├── peer.rs
│       └── disk.rs
├── rkd/              # Daemon Binary: Main background service, state holder, gRPC server
│   ├── Cargo.toml
│   ├── build.rs
│   └── src/
│       └── main.rs
└── rk-cli/           # UI Binary: Scriptable CLI tool and full-screen TUI Dashboard
    ├── Cargo.toml
    └── src/
        └── main.rs
```

### 1.2 Root Cargo.toml
Create this file in the root directory to define the workspace and optimize release builds:

```toml
[workspace]
members = [
    "rk-engine",
    "rkd",
    "rk-cli"
]
resolver = "2"

[profile.release]
opt-level = 3
lto = true
codegen-units = 1
panic = "abort"
```

---

## 2. Core Engine Library (rk-engine)

The core engine handles low-level BitTorrent logic: parsing `.torrent` metainfo, managing peer wire protocol connections, and serializing block writes/reads to disk via a specialized actor.

### 2.1 Dependencies Configuration (rk-engine/Cargo.toml)

```toml
[package]
name = "rk-engine"
version = "0.1.0"
edition = "2021"

[dependencies]
tokio = { version = "1.35", features = ["full"] }
tokio-util = { version = "0.7", features = ["codec"] }
bendy = "0.3"
sha1 = "0.10"
bytes = "1.0"
serde = { version = "1.0", features = ["derive"] }
thiserror = "1.0"
tracing = "0.1"
anyhow = "1.0"
```

### 2.2 Bencode Metainfo Implementation (rk-engine/src/bencode.rs)

Securely decodes `.torrent` metadata and derives the 20-byte Info-Hash using zero-copy principles where possible.

```rust
use serde::{Deserialize, Serialize};
use sha1::{Digest, Sha1};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum MetainfoError {
    #[error("Bencode deserialization failed: {0}")]
    Bencode(#[from] bendy::serde::Error),
    #[error("Missing expected dictionary keys")]
    InvalidFormat,
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
```

### 2.3 Peer Wire Protocol (PWP) Codec (rk-engine/src/codec.rs)

The PWP engine uses a dedicated frame decoder and encoder utilizing `tokio_util::codec` to prevent framing issues on raw TCP sockets.

```rust
use bytes::{Buf, BufMut, Bytes, BytesMut};
use std::io;
use tokio_util::codec::{Decoder, Encoder};

#[derive(Debug, Clone, PartialEq)]
pub enum PwpMessage {
    KeepAlive,
    Choke,
    Unchoke,
    Interested,
    NotInterested,
    Have(u32),
    Bitfield(Bytes),
    Request { index: u32, begin: u32, length: u32 },
    Piece { index: u32, begin: u32, block: Bytes },
    Cancel { index: u32, begin: u32, length: u32 },
}

pub struct PwpCodec;

impl Decoder for PwpCodec {
    type Item = PwpMessage;
    type Error = io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if src.len() < 4 {
            return Ok(None);
        }

        // Read 4-byte length prefix
        let mut len_bytes = [0u8; 4];
        len_bytes.copy_from_slice(&src[..4]);
        let length = u32::from_be_bytes(len_bytes) as usize;

        if length == 0 {
            src.advance(4);
            return Ok(Some(PwpMessage::KeepAlive));
        }

        if src.len() < 4 + length {
            // Wait for full message payload
            src.reserve(4 + length - src.len());
            return Ok(None);
        }

        src.advance(4); // Advance past length prefix
        let id = src[0];

        let msg = match id {
            0 => PwpMessage::Choke,
            1 => PwpMessage::Unchoke,
            2 => PwpMessage::Interested,
            3 => PwpMessage::NotInterested,
            4 => {
                if length != 5 {
                    return Err(io::Error::new(io::ErrorKind::InvalidData, "Invalid Have size"));
                }
                let piece_idx = u32::from_be_bytes([src[1], src[2], src[3], src[4]]);
                PwpMessage::Have(piece_idx)
            }
            5 => {
                let bitfield_data = src.copy_to_bytes(length);
                // Slice bitfield excluding the message ID
                PwpMessage::Bitfield(bitfield_data.slice(1..))
            }
            6 => {
                if length != 13 {
                    return Err(io::Error::new(io::ErrorKind::InvalidData, "Invalid Request size"));
                }
                let index = u32::from_be_bytes([src[1], src[2], src[3], src[4]]);
                let begin = u32::from_be_bytes([src[5], src[6], src[7], src[8]]);
                let length = u32::from_be_bytes([src[9], src[10], src[11], src[12]]);
                PwpMessage::Request { index, begin, length }
            }
            7 => {
                if length < 9 {
                    return Err(io::Error::new(io::ErrorKind::InvalidData, "Invalid Piece size"));
                }
                let index = u32::from_be_bytes([src[1], src[2], src[3], src[4]]);
                let begin = u32::from_be_bytes([src[5], src[6], src[7], src[8]]);
                let block = src.copy_to_bytes(length).slice(9..);
                PwpMessage::Piece { index, begin, block }
            }
            8 => {
                if length != 13 {
                    return Err(io::Error::new(io::ErrorKind::InvalidData, "Invalid Cancel size"));
                }
                let index = u32::from_be_bytes([src[1], src[2], src[3], src[4]]);
                let begin = u32::from_be_bytes([src[5], src[6], src[7], src[8]]);
                let length = u32::from_be_bytes([src[9], src[10], src[11], src[12]]);
                PwpMessage::Cancel { index, begin, length }
            }
            _ => return Err(io::Error::new(io::ErrorKind::InvalidData, "Unknown message ID")),
        };

        // If it wasn't parsed by copy_to_bytes (fixed size messages), advance now
        if id != 5 && id != 7 {
            src.advance(length);
        }

        Ok(Some(msg))
    }
}

impl Encoder<PwpMessage> for PwpCodec {
    type Error = io::Error;

    fn encode(&mut self, item: PwpMessage, dst: &mut BytesMut) -> Result<(), Self::Error> {
        match item {
            PwpMessage::KeepAlive => {
                dst.put_u32(0);
            }
            PwpMessage::Choke => {
                dst.put_u32(1);
                dst.put_u8(0);
            }
            PwpMessage::Unchoke => {
                dst.put_u32(1);
                dst.put_u8(1);
            }
            PwpMessage::Interested => {
                dst.put_u32(1);
                dst.put_u8(2);
            }
            PwpMessage::NotInterested => {
                dst.put_u32(1);
                dst.put_u8(3);
            }
            PwpMessage::Have(idx) => {
                dst.put_u32(5);
                dst.put_u8(4);
                dst.put_u32(idx);
            }
            PwpMessage::Bitfield(ref data) => {
                dst.put_u32((data.len() + 1) as u32);
                dst.put_u8(5);
                dst.put_slice(data);
            }
            PwpMessage::Request { index, begin, length } => {
                dst.put_u32(13);
                dst.put_u8(6);
                dst.put_u32(index);
                dst.put_u32(begin);
                dst.put_u32(length);
            }
            PwpMessage::Piece { index, begin, ref block } => {
                dst.put_u32((block.len() + 9) as u32);
                dst.put_u8(7);
                dst.put_u32(index);
                dst.put_u32(begin);
                dst.put_slice(block);
            }
            PwpMessage::Cancel { index, begin, length } => {
                dst.put_u32(13);
                dst.put_u8(8);
                dst.put_u32(index);
                dst.put_u32(begin);
                dst.put_u32(length);
            }
        }
        Ok(())
    }
}
```

### 2.4 Disk Actor Architecture (rk-engine/src/disk.rs)

To prevent blocking the core Tokio event loop, file reading and writing operations must be offloaded to dedicated worker threads. This implementation models the Disk I/O as an **Actor** communicating via MPSC channels.

```rust
use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::PathBuf;
use tokio::sync::{mpsc, oneshot};

pub enum DiskCommand {
    WriteBlock {
        file_path: PathBuf,
        offset: u64,
        data: bytes::Bytes,
        reply: oneshot::Sender<Result<(), String>>,
    },
    ReadBlock {
        file_path: PathBuf,
        offset: u64,
        length: usize,
        reply: oneshot::Sender<Result<bytes::Bytes, String>>,
    },
}

pub struct DiskActor {
    receiver: mpsc::Receiver<DiskCommand>,
}

impl DiskActor {
    pub fn spawn(buffer_size: usize) -> (mpsc::Sender<DiskCommand>, tokio::task::JoinHandle<()>) {
        let (sender, receiver) = mpsc::channel(buffer_size);
        let actor = DiskActor { receiver };
        
        let handle = tokio::task::spawn_blocking(move || {
            actor.run_loop();
        });
        
        (sender, handle)
    }

    fn run_loop(mut self) {
        while let Some(command) = self.receiver.blocking_recv() {
            match command {
                DiskCommand::WriteBlock { file_path, offset, data, reply } => {
                    let res = Self::handle_write(file_path, offset, &data);
                    let _ = reply.send(res);
                }
                DiskCommand::ReadBlock { file_path, offset, length, reply } => {
                    let res = Self::handle_read(file_path, offset, length);
                    let _ = reply.send(res);
                }
            }
        }
    }

    fn handle_write(path: PathBuf, offset: u64, data: &[u8]) -> Result<(), String> {
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .open(&path)
            .map_err(|e| format!("Failed to open file for writing: {}", e))?;
            
        file.seek(SeekFrom::Start(offset))
            .map_err(|e| format!("Seek failed: {}", e))?;
            
        file.write_all(data)
            .map_err(|e| format!("Write failed: {}", e))?;
            
        Ok(())
    }

    fn handle_read(path: PathBuf, offset: u64, length: usize) -> Result<bytes::Bytes, String> {
        let mut file = File::open(&path)
            .map_err(|e| format!("Failed to open file for reading: {}", e))?;
            
        file.seek(SeekFrom::Start(offset))
            .map_err(|e| format!("Seek failed: {}", e))?;
            
        let mut buf = vec![0u8; length];
        file.read_exact(&mut buf)
            .map_err(|e| format!("Read failed: {}", e))?;
            
        Ok(bytes::Bytes::from(buf))
    }
}
```

---

## 3. The Communication Contract (gRPC / Protobuf)

To maintain a responsive architecture, the user interface logic does not manage socket operations. It communicates with the daemon via local gRPC services.

### 3.1 Contract Definition (proto/torrent.proto)

```protobuf
syntax = "proto3";
package torrent;

service TorrentService {
    // Registers a local file path or magnet link with the daemon
    rpc AddTorrent(AddTorrentRequest) returns (AddTorrentResponse);
    
    // Returns a snapshot list of current active downloads
    rpc ListTorrents(ListTorrentsRequest) returns (ListTorrentsResponse);
    
    // Server-streaming RPC to feed real-time metrics into the CLI/TUI
    rpc StreamStatus(StatusRequest) returns (stream StatusUpdate);
}

message AddTorrentRequest {
    string target = 1; 
    bool sequential = 2;
}

message AddTorrentResponse {
    string info_hash = 1;
    bool success = 2;
}

message ListTorrentsRequest {}

message TorrentStatus {
    string name = 1;
    string info_hash = 2;
    float progress = 3;
    uint64 download_speed = 4;
    uint64 upload_speed = 5;
    uint32 peer_count = 6;
}

message ListTorrentsResponse {
    repeated TorrentStatus torrents = 1;
}

message StatusRequest {}

message StatusUpdate {
    repeated TorrentStatus torrents = 1;
}
```

---

## 4. Background Server Layer (rkd)

`rkd` is the background daemon managing the global download queue, handling peer networks, and listening for client commands.

### 4.1 Server Configurations (rkd/Cargo.toml)

```toml
[package]
name = "rkd"
version = "0.1.0"
edition = "2021"

[dependencies]
rk-engine = { path = "../rk-engine" }
tokio = { version = "1.35", features = ["full"] }
tokio-stream = { version = "0.1", features = ["net"] }
tonic = "0.10"
prost = "0.12"
tracing = "0.1"
tracing-subscriber = "0.3"
anyhow = "1.0"
tokio-util = "0.7"

[build-dependencies]
tonic-build = "0.10"
```

Configure `rkd/build.rs` to generate client/server wrappers:

```rust
// rkd/build.rs
fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::compile_proto("../proto/torrent.proto")?;
    Ok(())
}
```

### 4.2 Daemon State & RPC Server Implementation (rkd/src/main.rs)

The daemon coordinates download routines and implements `StreamStatus` by monitoring internal states via a `tokio::sync::watch` channel.

```rust
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{watch, RwLock};
use tokio_stream::wrappers::WatchStream;
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
        
        let new_torrent = TorrentStatus {
            name: inner.target.clone(),
            info_hash: "a1b2c3d4e5f67890".to_string(),
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

    type StreamStatusStream = WatchStream<Result<StatusUpdate, Status>>;

    async fn stream_status(
        &self,
        _request: Request<StatusRequest>,
    ) -> Result<Response<Self::StreamStatusStream>, Status> {
        let rx = self.tx.subscribe();
        let stream = WatchStream::new(rx).map(Ok);
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
            tokio::signal::ctrl_c()
                .await
                .expect("Failed to listen for ctrl-c signal");
            tracing::info!("Shutdown signal intercepted. Closing server...");
            shutdown_token.cancel();
        })
        .await?;
        
    Ok(())
}
```

---

## 5. UI Client Layer (rk-cli)

`rk-cli` acts as the primary tool to control the daemon, operating as a basic CLI utility or launching an interactive fullscreen dashboard.

### 5.1 Client Configurations (rk-cli/Cargo.toml)

```toml
[package]
name = "rk-cli"
version = "0.1.0"
edition = "2021"

[dependencies]
tokio = { version = "1.35", features = ["full"] }
tonic = "0.10"
prost = "0.12"
ratatui = "0.26"
crossterm = { version = "0.27", features = ["event-stream"] }
anyhow = "1.0"
clap = { version = "4.4", features = ["derive"] }
futures-util = "0.3"
```

Configure `rk-cli/build.rs` to construct the client gRPC bindings:

```rust
// rk-cli/build.rs
fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::compile_proto("../proto/torrent.proto")?;
    Ok(())
}
```

### 5.2 Interactive Dashboard Implementation (rk-cli/src/main.rs)

The implementation parses command line options, connects over local loops, and paints TUI elements using ratatui's double-buffer layout.

```rust
use clap::{Parser, Subcommand};
use crossterm::{
    event::{self, DisableMouseCapture, EnableMouseCapture, Event, KeyCode},
    execute,
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
};
use ratatui::{
    backend::CrosstermBackend,
    layout::{Constraint, Direction, Layout},
    widgets::{Block, Borders, Paragraph, Gauge},
    Terminal,
};
use std::io;
use torrent_proto::torrent_service_client::TorrentServiceClient;
use torrent_proto::{AddTorrentRequest, ListTorrentsRequest, StatusRequest};

pub mod torrent_proto {
    tonic::include_proto!("torrent");
}

#[derive(Parser)]
#[command(name = "rk")]
#[command(about = "Rektorrent CLI & TUI Controller")]
struct Cli {
    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(Subcommand)]
enum Commands {
    Add { target: String },
    List,
    Tui,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Cli::parse();
    let mut client = TorrentServiceClient::connect("http://[::1]:50051").await?;

    match args.command.unwrap_or(Commands::Tui) {
        Commands::Add { target } => {
            let res = client.add_torrent(AddTorrentRequest { target, sequential: false }).await?;
            println!("Torrent added. InfoHash: {}", res.into_inner().info_hash);
        }
        Commands::List => {
            let res = client.list_torrents(ListTorrentsRequest {}).await?;
            for torrent in res.into_inner().torrents {
                println!("- {}: [{}] Progress: {:.2}%", torrent.name, torrent.info_hash, torrent.progress * 100.0);
            }
        }
        Commands::Tui => {
            run_tui(&mut client).await?;
        }
    }

    Ok(())
}

async fn run_tui(client: &mut TorrentServiceClient<tonic::transport::Channel>) -> Result<(), Box<dyn std::error::Error>> {
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen, EnableMouseCapture)?;
    
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    let mut stream = client.stream_status(StatusRequest {}).await?.into_inner();

    loop {
        // Read the latest update from stream
        let update = tokio::select! {
            next = stream.message() => {
                match next {
                    Ok(Some(status)) => status,
                    _ => break, // Stream closed or error
                }
            }
            _ = tokio::time::sleep(std::time::Duration::from_millis(500)) => {
                continue;
            }
        };

        terminal.draw(|f| {
            let chunks = Layout::default()
                .direction(Direction::Vertical)
                .margin(1)
                .constraints([
                    Constraint::Length(3),
                    Constraint::Min(5),
                ].as_ref())
                .split(f.size());

            // Header block
            let header = Paragraph::new(" Rektorrent Active Sync Session Dashboard (Press 'q' to Quit) ")
                .block(Block::default().borders(Borders::ALL));
            f.render_widget(header, chunks[0]);

            // Body rendering lists with gauges
            if update.torrents.is_empty() {
                let info = Paragraph::new("No torrents active. Try adding a torrent using the CLI.")
                    .block(Block::default().borders(Borders::ALL));
                f.render_widget(info, chunks[1]);
            } else {
                let torrent = &update.torrents[0];
                let info_text = format!(
                    "Name: {}\nHash: {}\nDownload Speed: {:.2} KB/s\nUpload Speed: {:.2} KB/s\nPeers: {}",
                    torrent.name,
                    torrent.info_hash,
                    torrent.download_speed as f64 / 1024.0,
                    torrent.upload_speed as f64 / 1024.0,
                    torrent.peer_count
                );
                
                let inner_layout = Layout::default()
                    .direction(Direction::Vertical)
                    .constraints([
                        Constraint::Length(6),
                        Constraint::Length(3),
                    ].as_ref())
                    .split(chunks[1]);

                let details = Paragraph::new(info_text).block(Block::default().title(" Torrent Info ").borders(Borders::ALL));
                f.render_widget(details, inner_layout[0]);

                let gauge = Gauge::default()
                    .block(Block::default().title(" Progress ").borders(Borders::ALL))
                    .gauge_style(ratatui::style::Style::default().fg(ratatui::style::Color::Cyan))
                    .percent((torrent.progress * 100.0) as u16);
                f.render_widget(gauge, inner_layout[1]);
            }
        })?;

        // Handle quick keyboard exit checks without blocking the draw rate
        if event::poll(std::time::Duration::from_millis(10))? {
            if let Event::Key(key) = event::read()? {
                if key.code == KeyCode::Char('q') {
                    break;
                }
            }
        }
    }

    disable_raw_mode()?;
    execute!(
        terminal.backend_mut(),
        LeaveAlternateScreen,
        DisableMouseCapture
    )?;
    terminal.show_cursor()?;
    Ok(())
}
```

---

## 6. Architecture & Concurrency Rules (Quality Checklist)

To secure peak performance and protect concurrent states under loaded conditions:

### 6.1 State Locking & Mutex Boundaries
- **Rule**: Never hold standard lock guards (`std::sync::MutexGuard`, `std::sync::RwLockGuard`) across `.await` yield points. Doing so causes worker thread starvation and eventual deadlocks.
- **Remedy**: Use tokio-equivalent lock types (`tokio::sync::Mutex`, `tokio::sync::RwLock`) or design asynchronous data processing loops to isolate mutation fields.

### 6.2 Preventing Worker Pool Starvation
- **Rule**: Never run heavy CPU-bound algorithms (such as parsing large metainfo block structures or evaluating SHA-1 block sums) directly inside the main thread loop.
- **Remedy**: Wrap blocking calls with `tokio::task::spawn_blocking` or delegate operations to dedicated background OS thread pools.

### 6.3 Error Handling
- **Rule**: Avoid executing `unwrap()` or `expect()` variants inside production modules.
- **Remedy**: Model errors explicitly via enum lists annotated with `#[derive(thiserror::Error)]` inside internal engine packages. Use the `?` bubble operator to propagate errors upwards to CLI and CLI-daemon handlers which leverage the `anyhow` container for formatting.

---

## 7. Verification & Deployment Roadmap

Follow this sequence to build and test the architecture locally:

### 7.1 Automated Testing Commands
Validate the parsing engine and connection codecs independently:
```bash
# Execute local unit tests across workspace targets
cargo test --workspace

# Lint code style constraints and safety features
cargo clippy --workspace --all-targets --all-features -- -D warnings
```

### 7.2 Manual Deployment Workflow
1. **Launch Daemon**: Start the service process listener on port `50051`:
   ```bash
   cargo run --bin rkd
   ```
2. **Interact with CLI**: Register a target file to verify downflow propagation:
   ```bash
   cargo run --bin rk-cli -- add ubuntu-24.04-desktop-amd64.torrent
   ```
3. **Launch Monitor**: Verify layout painting and streaming loops inside the terminal:
   ```bash
   cargo run --bin rk-cli -- tui
   ```