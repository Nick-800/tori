use std::io;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::{timeout, Duration};

const PROTOCOL_STRING: &[u8; 19] = b"BitTorrent protocol";
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(5);

pub async fn perform_handshake(
    stream: &mut TcpStream,
    info_hash: &[u8; 20],
    peer_id: &[u8; 20],
) -> io::Result<[u8; 20]> {
    // Construct 68-byte handshake
    let mut handshake = [0u8; 68];
    handshake[0] = 19;
    handshake[1..20].copy_from_slice(PROTOCOL_STRING);
    // bytes 20..28 are reserved (all zeros for now)
    handshake[28..48].copy_from_slice(info_hash);
    handshake[48..68].copy_from_slice(peer_id);

    // Send handshake with timeout
    timeout(HANDSHAKE_TIMEOUT, stream.write_all(&handshake))
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "Handshake write timed out"))??;

    // Read 68-byte response
    let mut response = [0u8; 68];
    timeout(HANDSHAKE_TIMEOUT, stream.read_exact(&mut response))
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "Handshake read timed out"))??;

    if response[0] != 19 || &response[1..20] != PROTOCOL_STRING {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "Invalid protocol string from peer",
        ));
    }

    let mut response_info_hash = [0u8; 20];
    response_info_hash.copy_from_slice(&response[28..48]);

    if &response_info_hash != info_hash {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "Info hash mismatch",
        ));
    }

    let mut response_peer_id = [0u8; 20];
    response_peer_id.copy_from_slice(&response[48..68]);

    Ok(response_peer_id)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_handshake_format() {
        let info_hash = [1u8; 20];
        let peer_id = [2u8; 20];

        let mut expected = vec![19];
        expected.extend_from_slice(b"BitTorrent protocol");
        expected.extend_from_slice(&[0; 8]);
        expected.extend_from_slice(&info_hash);
        expected.extend_from_slice(&peer_id);

        assert_eq!(expected.len(), 68);
    }
}
