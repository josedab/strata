//! NFS RPC protocol handling.
//!
//! This module handles the low-level RPC protocol for NFSv4,
//! including XDR encoding/decoding and RPC message framing.

use super::error::NfsStatus;
use serde::{Deserialize, Serialize};
use std::io::{self, Read, Write};

/// NFS RPC program number.
pub const NFS_PROGRAM: u32 = 100003;

/// NFSv4 version number.
pub const NFS_V4: u32 = 4;

/// RPC version.
pub const RPC_VERSION: u32 = 2;

/// RPC message types.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u32)]
pub enum RpcMsgType {
    /// Call message.
    Call = 0,
    /// Reply message.
    Reply = 1,
}

/// RPC reply status.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u32)]
pub enum RpcReplyStatus {
    /// Message accepted.
    Accepted = 0,
    /// Message denied.
    Denied = 1,
}

/// RPC accept status.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u32)]
pub enum RpcAcceptStatus {
    /// Success.
    Success = 0,
    /// Program unavailable.
    ProgUnavail = 1,
    /// Program version mismatch.
    ProgMismatch = 2,
    /// Procedure unavailable.
    ProcUnavail = 3,
    /// Garbage arguments.
    GarbageArgs = 4,
    /// System error.
    SystemErr = 5,
}

/// RPC authentication flavor.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u32)]
pub enum AuthFlavor {
    /// No authentication.
    None = 0,
    /// Unix/System authentication.
    Unix = 1,
    /// Short authentication.
    Short = 2,
    /// DES authentication.
    Des = 3,
    /// Kerberos v4.
    Kerb4 = 4,
    /// RPCSEC_GSS.
    RpcsecGss = 6,
}

/// RPC credentials.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OpaqueAuth {
    /// Authentication flavor.
    pub flavor: u32,
    /// Authentication body.
    pub body: Vec<u8>,
}

impl OpaqueAuth {
    /// Create null authentication.
    pub fn null() -> Self {
        Self {
            flavor: AuthFlavor::None as u32,
            body: Vec::new(),
        }
    }

    /// Create Unix authentication.
    pub fn unix(uid: u32, gid: u32, gids: &[u32], machine_name: &str) -> Self {
        let mut body = Vec::new();
        // Stamp (timestamp)
        body.extend_from_slice(&(0u32).to_be_bytes());
        // Machine name (XDR string)
        let name_bytes = machine_name.as_bytes();
        body.extend_from_slice(&(name_bytes.len() as u32).to_be_bytes());
        body.extend_from_slice(name_bytes);
        // Pad to 4-byte boundary
        let pad = (4 - (name_bytes.len() % 4)) % 4;
        body.extend(vec![0u8; pad]);
        // UID
        body.extend_from_slice(&uid.to_be_bytes());
        // GID
        body.extend_from_slice(&gid.to_be_bytes());
        // Supplementary GIDs
        body.extend_from_slice(&(gids.len() as u32).to_be_bytes());
        for gid in gids {
            body.extend_from_slice(&gid.to_be_bytes());
        }

        Self {
            flavor: AuthFlavor::Unix as u32,
            body,
        }
    }
}

/// RPC call message.
#[derive(Debug, Clone)]
pub struct RpcCall {
    /// Transaction ID.
    pub xid: u32,
    /// RPC version (should be 2).
    pub rpc_vers: u32,
    /// Program number.
    pub prog: u32,
    /// Program version.
    pub vers: u32,
    /// Procedure number.
    pub proc_num: u32,
    /// Credentials.
    pub cred: OpaqueAuth,
    /// Verifier.
    pub verf: OpaqueAuth,
}

impl RpcCall {
    /// Create a new RPC call.
    pub fn new(xid: u32, proc_num: u32) -> Self {
        Self {
            xid,
            rpc_vers: RPC_VERSION,
            prog: NFS_PROGRAM,
            vers: NFS_V4,
            proc_num,
            cred: OpaqueAuth::null(),
            verf: OpaqueAuth::null(),
        }
    }
}

/// RPC reply message.
#[derive(Debug, Clone)]
pub struct RpcReply {
    /// Transaction ID.
    pub xid: u32,
    /// Reply status.
    pub reply_status: RpcReplyStatus,
    /// Accept status (if accepted).
    pub accept_status: Option<RpcAcceptStatus>,
    /// Verifier (if accepted).
    pub verf: Option<OpaqueAuth>,
    /// Reply data.
    pub data: Vec<u8>,
}

impl RpcReply {
    /// Create a successful reply.
    pub fn success(xid: u32, data: Vec<u8>) -> Self {
        Self {
            xid,
            reply_status: RpcReplyStatus::Accepted,
            accept_status: Some(RpcAcceptStatus::Success),
            verf: Some(OpaqueAuth::null()),
            data,
        }
    }

    /// Create an error reply.
    pub fn error(xid: u32, status: RpcAcceptStatus) -> Self {
        Self {
            xid,
            reply_status: RpcReplyStatus::Accepted,
            accept_status: Some(status),
            verf: Some(OpaqueAuth::null()),
            data: Vec::new(),
        }
    }

    /// Create a denied reply.
    pub fn denied(xid: u32) -> Self {
        Self {
            xid,
            reply_status: RpcReplyStatus::Denied,
            accept_status: None,
            verf: None,
            data: Vec::new(),
        }
    }
}

/// RPC message (either call or reply).
#[derive(Debug, Clone)]
pub enum RpcMessage {
    /// Call message.
    Call(RpcCall),
    /// Reply message.
    Reply(RpcReply),
}

/// XDR codec for encoding/decoding NFS data.
pub struct XdrCodec {
    /// Internal buffer.
    buffer: Vec<u8>,
    /// Current position for reading.
    position: usize,
}

impl XdrCodec {
    /// Create a new empty codec.
    pub fn new() -> Self {
        Self {
            buffer: Vec::new(),
            position: 0,
        }
    }

    /// Create a codec from existing data.
    pub fn from_bytes(data: Vec<u8>) -> Self {
        Self {
            buffer: data,
            position: 0,
        }
    }

    /// Get the encoded data.
    pub fn into_bytes(self) -> Vec<u8> {
        self.buffer
    }

    /// Get a reference to the buffer.
    pub fn as_bytes(&self) -> &[u8] {
        &self.buffer
    }

    /// Get remaining bytes.
    pub fn remaining(&self) -> usize {
        self.buffer.len().saturating_sub(self.position)
    }

    // === Encoding methods ===

    /// Encode a u32.
    pub fn encode_u32(&mut self, value: u32) {
        self.buffer.extend_from_slice(&value.to_be_bytes());
    }

    /// Encode an i32.
    pub fn encode_i32(&mut self, value: i32) {
        self.buffer.extend_from_slice(&value.to_be_bytes());
    }

    /// Encode a u64.
    pub fn encode_u64(&mut self, value: u64) {
        self.buffer.extend_from_slice(&value.to_be_bytes());
    }

    /// Encode an i64.
    pub fn encode_i64(&mut self, value: i64) {
        self.buffer.extend_from_slice(&value.to_be_bytes());
    }

    /// Encode a bool.
    pub fn encode_bool(&mut self, value: bool) {
        self.encode_u32(if value { 1 } else { 0 });
    }

    /// Encode opaque data (fixed length).
    pub fn encode_opaque_fixed(&mut self, data: &[u8]) {
        self.buffer.extend_from_slice(data);
        // Pad to 4-byte boundary
        let pad = (4 - (data.len() % 4)) % 4;
        self.buffer.extend(vec![0u8; pad]);
    }

    /// Encode opaque data (variable length).
    pub fn encode_opaque(&mut self, data: &[u8]) {
        self.encode_u32(data.len() as u32);
        self.encode_opaque_fixed(data);
    }

    /// Encode a string.
    pub fn encode_string(&mut self, s: &str) {
        self.encode_opaque(s.as_bytes());
    }

    /// Encode an array of u32.
    pub fn encode_u32_array(&mut self, values: &[u32]) {
        self.encode_u32(values.len() as u32);
        for v in values {
            self.encode_u32(*v);
        }
    }

    // === Decoding methods ===

    /// Decode a u32.
    pub fn decode_u32(&mut self) -> io::Result<u32> {
        if self.position + 4 > self.buffer.len() {
            return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "Not enough data"));
        }
        let bytes: [u8; 4] = self.buffer[self.position..self.position + 4]
            .try_into()
            .unwrap();
        self.position += 4;
        Ok(u32::from_be_bytes(bytes))
    }

    /// Decode an i32.
    pub fn decode_i32(&mut self) -> io::Result<i32> {
        if self.position + 4 > self.buffer.len() {
            return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "Not enough data"));
        }
        let bytes: [u8; 4] = self.buffer[self.position..self.position + 4]
            .try_into()
            .unwrap();
        self.position += 4;
        Ok(i32::from_be_bytes(bytes))
    }

    /// Decode a u64.
    pub fn decode_u64(&mut self) -> io::Result<u64> {
        if self.position + 8 > self.buffer.len() {
            return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "Not enough data"));
        }
        let bytes: [u8; 8] = self.buffer[self.position..self.position + 8]
            .try_into()
            .unwrap();
        self.position += 8;
        Ok(u64::from_be_bytes(bytes))
    }

    /// Decode an i64.
    pub fn decode_i64(&mut self) -> io::Result<i64> {
        if self.position + 8 > self.buffer.len() {
            return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "Not enough data"));
        }
        let bytes: [u8; 8] = self.buffer[self.position..self.position + 8]
            .try_into()
            .unwrap();
        self.position += 8;
        Ok(i64::from_be_bytes(bytes))
    }

    /// Decode a bool.
    pub fn decode_bool(&mut self) -> io::Result<bool> {
        Ok(self.decode_u32()? != 0)
    }

    /// Decode opaque data (fixed length).
    pub fn decode_opaque_fixed(&mut self, len: usize) -> io::Result<Vec<u8>> {
        let padded_len = ((len + 3) / 4) * 4;
        if self.position + padded_len > self.buffer.len() {
            return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "Not enough data"));
        }
        let data = self.buffer[self.position..self.position + len].to_vec();
        self.position += padded_len;
        Ok(data)
    }

    /// Decode opaque data (variable length).
    pub fn decode_opaque(&mut self) -> io::Result<Vec<u8>> {
        let len = self.decode_u32()? as usize;
        self.decode_opaque_fixed(len)
    }

    /// Decode a string.
    pub fn decode_string(&mut self) -> io::Result<String> {
        let bytes = self.decode_opaque()?;
        String::from_utf8(bytes)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
    }

    /// Decode an array of u32.
    pub fn decode_u32_array(&mut self) -> io::Result<Vec<u32>> {
        let count = self.decode_u32()? as usize;
        let mut values = Vec::with_capacity(count);
        for _ in 0..count {
            values.push(self.decode_u32()?);
        }
        Ok(values)
    }

    /// Decode OpaqueAuth.
    pub fn decode_opaque_auth(&mut self) -> io::Result<OpaqueAuth> {
        let flavor = self.decode_u32()?;
        let body = self.decode_opaque()?;
        Ok(OpaqueAuth { flavor, body })
    }

    /// Encode OpaqueAuth.
    pub fn encode_opaque_auth(&mut self, auth: &OpaqueAuth) {
        self.encode_u32(auth.flavor);
        self.encode_opaque(&auth.body);
    }

    /// Decode an RPC call.
    pub fn decode_rpc_call(&mut self) -> io::Result<RpcCall> {
        let xid = self.decode_u32()?;
        let msg_type = self.decode_u32()?;
        if msg_type != RpcMsgType::Call as u32 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Expected RPC call message",
            ));
        }

        let rpc_vers = self.decode_u32()?;
        let prog = self.decode_u32()?;
        let vers = self.decode_u32()?;
        let proc_num = self.decode_u32()?;
        let cred = self.decode_opaque_auth()?;
        let verf = self.decode_opaque_auth()?;

        Ok(RpcCall {
            xid,
            rpc_vers,
            prog,
            vers,
            proc_num,
            cred,
            verf,
        })
    }

    /// Encode an RPC reply.
    pub fn encode_rpc_reply(&mut self, reply: &RpcReply) {
        self.encode_u32(reply.xid);
        self.encode_u32(RpcMsgType::Reply as u32);
        self.encode_u32(reply.reply_status as u32);

        if reply.reply_status == RpcReplyStatus::Accepted {
            if let Some(ref verf) = reply.verf {
                self.encode_opaque_auth(verf);
            }
            if let Some(status) = reply.accept_status {
                self.encode_u32(status as u32);
            }
            self.buffer.extend_from_slice(&reply.data);
        }
    }
}

impl Default for XdrCodec {
    fn default() -> Self {
        Self::new()
    }
}

/// NFS protocol handler.
pub struct NfsProtocol;

impl NfsProtocol {
    /// NFSv4 procedure numbers.
    pub const PROC_NULL: u32 = 0;
    pub const PROC_COMPOUND: u32 = 1;

    /// Read a record-marked message.
    pub fn read_record<R: Read>(reader: &mut R) -> io::Result<Vec<u8>> {
        let mut result = Vec::new();
        loop {
            // Read record fragment header (4 bytes)
            let mut header = [0u8; 4];
            reader.read_exact(&mut header)?;

            let header_val = u32::from_be_bytes(header);
            let last_fragment = (header_val & 0x80000000) != 0;
            let length = (header_val & 0x7FFFFFFF) as usize;

            // Read fragment data
            let mut fragment = vec![0u8; length];
            reader.read_exact(&mut fragment)?;
            result.extend_from_slice(&fragment);

            if last_fragment {
                break;
            }
        }
        Ok(result)
    }

    /// Write a record-marked message.
    pub fn write_record<W: Write>(writer: &mut W, data: &[u8]) -> io::Result<()> {
        // Write as a single fragment with last-fragment bit set
        let header = 0x80000000 | (data.len() as u32);
        writer.write_all(&header.to_be_bytes())?;
        writer.write_all(data)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_xdr_encode_decode_u32() {
        let mut codec = XdrCodec::new();
        codec.encode_u32(0x12345678);

        let mut decoder = XdrCodec::from_bytes(codec.into_bytes());
        assert_eq!(decoder.decode_u32().unwrap(), 0x12345678);
    }

    #[test]
    fn test_xdr_encode_decode_u64() {
        let mut codec = XdrCodec::new();
        codec.encode_u64(0x123456789ABCDEF0);

        let mut decoder = XdrCodec::from_bytes(codec.into_bytes());
        assert_eq!(decoder.decode_u64().unwrap(), 0x123456789ABCDEF0);
    }

    #[test]
    fn test_xdr_encode_decode_string() {
        let mut codec = XdrCodec::new();
        codec.encode_string("hello world");

        let mut decoder = XdrCodec::from_bytes(codec.into_bytes());
        assert_eq!(decoder.decode_string().unwrap(), "hello world");
    }

    #[test]
    fn test_xdr_encode_decode_opaque() {
        let mut codec = XdrCodec::new();
        let data = vec![1, 2, 3, 4, 5];
        codec.encode_opaque(&data);

        let mut decoder = XdrCodec::from_bytes(codec.into_bytes());
        assert_eq!(decoder.decode_opaque().unwrap(), data);
    }

    #[test]
    fn test_xdr_padding() {
        let mut codec = XdrCodec::new();
        // 5 bytes needs 3 bytes of padding
        codec.encode_opaque(&[1, 2, 3, 4, 5]);

        // Length (4) + data (5) + padding (3) = 12 bytes
        assert_eq!(codec.as_bytes().len(), 12);
    }

    #[test]
    fn test_rpc_reply_encode() {
        let reply = RpcReply::success(12345, vec![1, 2, 3, 4]);

        let mut codec = XdrCodec::new();
        codec.encode_rpc_reply(&reply);

        let bytes = codec.into_bytes();
        assert!(!bytes.is_empty());
    }

    #[test]
    fn test_opaque_auth_unix() {
        let auth = OpaqueAuth::unix(1000, 1000, &[100, 101], "localhost");
        assert_eq!(auth.flavor, AuthFlavor::Unix as u32);
        assert!(!auth.body.is_empty());
    }
}
