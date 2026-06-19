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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_keepalive_codec() {
        let mut codec = PwpCodec;
        let mut buf = BytesMut::new();
        codec.encode(PwpMessage::KeepAlive, &mut buf).unwrap();
        assert_eq!(buf.to_vec(), vec![0, 0, 0, 0]);

        let decoded = codec.decode(&mut buf).unwrap().unwrap();
        assert_eq!(decoded, PwpMessage::KeepAlive);
    }

    #[test]
    fn test_choke_codec() {
        let mut codec = PwpCodec;
        let mut buf = BytesMut::new();
        codec.encode(PwpMessage::Choke, &mut buf).unwrap();
        assert_eq!(buf.to_vec(), vec![0, 0, 0, 1, 0]);

        let decoded = codec.decode(&mut buf).unwrap().unwrap();
        assert_eq!(decoded, PwpMessage::Choke);
    }
}
