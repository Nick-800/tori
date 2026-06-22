use super::{AnnounceRequest, AnnounceResponse, Tracker, TrackerError};
use async_trait::async_trait;
use rand::Rng;
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::time::Duration;
use tokio::net::UdpSocket;
use tokio::time::timeout;
use url::Url;
use bytes::{Buf, BufMut, BytesMut};

const CONNECT_ACTION: u32 = 0;
const ANNOUNCE_ACTION: u32 = 1;
const PROTOCOL_ID: u64 = 0x41727101980;

pub struct UdpTracker {
    url: Url,
}

impl UdpTracker {
    pub fn new(url: Url) -> Self {
        Self { url }
    }

    async fn connect(&self, socket: &UdpSocket, dest: &SocketAddr) -> Result<u64, TrackerError> {
        let mut req = BytesMut::with_capacity(16);
        let transaction_id: u32 = rand::thread_rng().gen();
        
        req.put_u64(PROTOCOL_ID);
        req.put_u32(CONNECT_ACTION);
        req.put_u32(transaction_id);

        socket.send_to(&req, dest).await.map_err(|e| TrackerError::Network(e.to_string()))?;

        let mut buf = [0u8; 16];
        let (len, _src) = timeout(Duration::from_secs(5), socket.recv_from(&mut buf))
            .await
            .map_err(|_| TrackerError::Network("Connect timeout".into()))?
            .map_err(|e| TrackerError::Network(e.to_string()))?;

        if len < 16 {
            return Err(TrackerError::Parse("Invalid connect response length".into()));
        }

        let mut resp = &buf[..];
        let action = resp.get_u32();
        let res_transaction_id = resp.get_u32();
        let connection_id = resp.get_u64();

        if action == 3 { // Error
            return Err(TrackerError::TrackerFailure("Tracker returned error during connect".into()));
        }
        if action != CONNECT_ACTION || res_transaction_id != transaction_id {
            return Err(TrackerError::Parse("Invalid connect response".into()));
        }

        Ok(connection_id)
    }

    async fn announce_udp(&self, socket: &UdpSocket, dest: &SocketAddr, connection_id: u64, req: &AnnounceRequest) -> Result<AnnounceResponse, TrackerError> {
        let mut buf = BytesMut::with_capacity(98);
        let transaction_id: u32 = rand::thread_rng().gen();
        let key: u32 = rand::thread_rng().gen();

        buf.put_u64(connection_id);
        buf.put_u32(ANNOUNCE_ACTION);
        buf.put_u32(transaction_id);
        buf.put_slice(&req.info_hash);
        buf.put_slice(&req.peer_id);
        buf.put_u64(req.downloaded);
        buf.put_u64(req.left);
        buf.put_u64(req.uploaded);
        buf.put_u32(0); // event = none
        buf.put_u32(0); // ip = 0 (default)
        buf.put_u32(key); // key
        buf.put_i32(-1); // num_want = default
        buf.put_u16(req.port); // port

        socket.send_to(&buf, dest).await.map_err(|e| TrackerError::Network(e.to_string()))?;

        let mut resp_buf = vec![0u8; 65536]; // Max UDP packet size
        let (len, _src) = timeout(Duration::from_secs(5), socket.recv_from(&mut resp_buf))
            .await
            .map_err(|_| TrackerError::Network("Announce timeout".into()))?
            .map_err(|e| TrackerError::Network(e.to_string()))?;

        if len < 20 {
            return Err(TrackerError::Parse("Invalid announce response length".into()));
        }

        let mut resp = &resp_buf[..len];
        let action = resp.get_u32();
        let res_transaction_id = resp.get_u32();

        if action == 3 {
            let err_msg = String::from_utf8_lossy(resp).to_string();
            return Err(TrackerError::TrackerFailure(err_msg));
        }

        if action != ANNOUNCE_ACTION || res_transaction_id != transaction_id {
            return Err(TrackerError::Parse("Invalid announce response".into()));
        }

        let interval = resp.get_u32();
        let _leechers = resp.get_u32();
        let _seeders = resp.get_u32();

        let mut peers = Vec::new();
        while resp.remaining() >= 6 {
            let ip = Ipv4Addr::new(resp.get_u8(), resp.get_u8(), resp.get_u8(), resp.get_u8());
            let port = resp.get_u16();
            peers.push(SocketAddr::V4(SocketAddrV4::new(ip, port)));
        }

        Ok(AnnounceResponse {
            interval,
            peers,
        })
    }
}

#[async_trait]
impl Tracker for UdpTracker {
    async fn announce(&self, req: AnnounceRequest) -> Result<AnnounceResponse, TrackerError> {
        let host = self.url.host_str().ok_or_else(|| TrackerError::Parse("Missing host in URL".into()))?;
        let port = self.url.port().unwrap_or(80);
        
        let addr_str = format!("{}:{}", host, port);
        // Resolve DNS and pick first IPv4
        let addrs = tokio::net::lookup_host(&addr_str)
            .await
            .map_err(|e| TrackerError::Network(format!("DNS resolution failed for {}: {}", addr_str, e)))?;
            
        let dest = addrs.into_iter()
            .find(|a| a.is_ipv4())
            .ok_or_else(|| TrackerError::Network("No IPv4 address found".into()))?;

        // Bind to any local UDP port
        let socket = UdpSocket::bind("0.0.0.0:0")
            .await
            .map_err(|e| TrackerError::Network(e.to_string()))?;

        let connection_id = self.connect(&socket, &dest).await?;
        self.announce_udp(&socket, &dest, connection_id, &req).await
    }
}
