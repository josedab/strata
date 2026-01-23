//! NFS server implementation.
//!
//! This module provides the TCP server that listens for NFS RPC requests
//! and dispatches them to the appropriate handlers.

use super::compound::CompoundRequest;
use super::error::NfsError;
use super::operations::NfsOperations;
use super::protocol::{NfsProtocol, RpcAcceptStatus, RpcCall, RpcReply, XdrCodec};
use super::types::{Attr4, AttrBitmap, ChangeInfo4, Stateid4, Verifier4};
use super::NfsGatewayState;
use crate::error::{Result, StrataError};
use std::io::{Read, Write};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tracing::{debug, error, info, warn};

/// NFS server configuration.
#[derive(Debug, Clone)]
pub struct NfsServerConfig {
    /// Address to bind to.
    pub bind_addr: SocketAddr,
    /// Maximum concurrent connections.
    pub max_connections: usize,
    /// Connection read timeout in seconds.
    pub read_timeout_secs: u64,
    /// Connection write timeout in seconds.
    pub write_timeout_secs: u64,
}

impl Default for NfsServerConfig {
    fn default() -> Self {
        Self {
            bind_addr: "0.0.0.0:2049".parse().unwrap(),
            max_connections: 1000,
            read_timeout_secs: 60,
            write_timeout_secs: 60,
        }
    }
}

/// NFS server.
pub struct NfsServer {
    /// Gateway state.
    state: Arc<NfsGatewayState>,
    /// Operations handler.
    operations: Arc<NfsOperations>,
}

impl NfsServer {
    /// Create a new NFS server.
    pub fn new(state: Arc<NfsGatewayState>) -> Result<Self> {
        let operations = Arc::new(NfsOperations::new(
            state.metadata.clone(),
            state.data.clone(),
            Arc::clone(&state.session_manager),
            Arc::clone(&state.state_manager),
            Arc::clone(&state.filehandle_gen),
        ));

        Ok(Self { state, operations })
    }

    /// Run the NFS server.
    pub async fn run(&self) -> Result<()> {
        let listener = TcpListener::bind(self.state.config.bind_addr)
            .await
            .map_err(|e| StrataError::Network(format!("Failed to bind: {}", e)))?;

        info!(addr = %self.state.config.bind_addr, "NFS server listening");

        // Spawn cleanup task
        let state = Arc::clone(&self.state);
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(60));
            loop {
                interval.tick().await;
                state.session_manager.cleanup_expired();
                state.state_manager.cleanup_expired();
            }
        });

        loop {
            match listener.accept().await {
                Ok((stream, addr)) => {
                    debug!(addr = %addr, "New NFS connection");
                    let operations = Arc::clone(&self.operations);
                    tokio::spawn(async move {
                        if let Err(e) = Self::handle_connection(stream, operations).await {
                            warn!(addr = %addr, error = %e, "Connection error");
                        }
                    });
                }
                Err(e) => {
                    error!(error = %e, "Accept error");
                }
            }
        }
    }

    /// Handle a single connection.
    async fn handle_connection(
        mut stream: TcpStream,
        operations: Arc<NfsOperations>,
    ) -> Result<()> {
        loop {
            // Read record-marked message
            let request_data = match Self::read_record(&mut stream).await {
                Ok(data) => data,
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                    debug!("Client disconnected");
                    return Ok(());
                }
                Err(e) => {
                    return Err(StrataError::Network(format!("Read error: {}", e)));
                }
            };

            // Parse RPC call
            let mut decoder = XdrCodec::from_bytes(request_data);
            let rpc_call = match decoder.decode_rpc_call() {
                Ok(call) => call,
                Err(e) => {
                    warn!(error = %e, "Invalid RPC call");
                    continue;
                }
            };

            // Process the call
            let reply = Self::process_call(&operations, rpc_call, &mut decoder).await;

            // Encode and send reply
            let mut encoder = XdrCodec::new();
            encoder.encode_rpc_reply(&reply);
            let reply_data = encoder.into_bytes();

            if let Err(e) = Self::write_record(&mut stream, &reply_data).await {
                return Err(StrataError::Network(format!("Write error: {}", e)));
            }
        }
    }

    /// Process an RPC call.
    async fn process_call(
        operations: &NfsOperations,
        call: RpcCall,
        decoder: &mut XdrCodec,
    ) -> RpcReply {
        // Validate RPC version
        if call.rpc_vers != 2 {
            return RpcReply::error(call.xid, RpcAcceptStatus::ProgMismatch);
        }

        // Validate program and version
        if call.prog != super::protocol::NFS_PROGRAM {
            return RpcReply::error(call.xid, RpcAcceptStatus::ProgUnavail);
        }

        if call.vers != super::protocol::NFS_V4 {
            return RpcReply::error(call.xid, RpcAcceptStatus::ProgMismatch);
        }

        // Process based on procedure
        match call.proc_num {
            NfsProtocol::PROC_NULL => {
                // NULL procedure - just return success
                RpcReply::success(call.xid, Vec::new())
            }
            NfsProtocol::PROC_COMPOUND => {
                // COMPOUND procedure
                match Self::decode_compound(decoder) {
                    Ok(compound_req) => {
                        match operations.process_compound(compound_req).await {
                            Ok(compound_resp) => {
                                let reply_data = Self::encode_compound_response(&compound_resp);
                                RpcReply::success(call.xid, reply_data)
                            }
                            Err(e) => {
                                error!(error = %e, "COMPOUND processing failed");
                                RpcReply::error(call.xid, RpcAcceptStatus::SystemErr)
                            }
                        }
                    }
                    Err(e) => {
                        warn!(error = %e, "Invalid COMPOUND request");
                        RpcReply::error(call.xid, RpcAcceptStatus::GarbageArgs)
                    }
                }
            }
            _ => {
                warn!(proc = call.proc_num, "Unknown NFS procedure");
                RpcReply::error(call.xid, RpcAcceptStatus::ProcUnavail)
            }
        }
    }

    /// Read a record-marked message.
    async fn read_record(stream: &mut TcpStream) -> std::io::Result<Vec<u8>> {
        let mut result = Vec::new();

        loop {
            // Read fragment header
            let mut header = [0u8; 4];
            stream.read_exact(&mut header).await?;

            let header_val = u32::from_be_bytes(header);
            let last_fragment = (header_val & 0x80000000) != 0;
            let length = (header_val & 0x7FFFFFFF) as usize;

            // Sanity check on length
            if length > 1024 * 1024 {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "Fragment too large",
                ));
            }

            // Read fragment data
            let mut fragment = vec![0u8; length];
            stream.read_exact(&mut fragment).await?;
            result.extend_from_slice(&fragment);

            if last_fragment {
                break;
            }
        }

        Ok(result)
    }

    /// Write a record-marked message.
    async fn write_record(stream: &mut TcpStream, data: &[u8]) -> std::io::Result<()> {
        // Write as single fragment with last-fragment bit
        let header = 0x80000000 | (data.len() as u32);
        stream.write_all(&header.to_be_bytes()).await?;
        stream.write_all(data).await?;
        stream.flush().await?;
        Ok(())
    }

    /// Decode a COMPOUND request.
    fn decode_compound(decoder: &mut XdrCodec) -> std::io::Result<CompoundRequest> {
        let tag = decoder.decode_string()?;
        let minorversion = decoder.decode_u32()?;
        let num_ops = decoder.decode_u32()? as usize;

        // Sanity check
        if num_ops > 100 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Too many operations",
            ));
        }

        let mut request = CompoundRequest {
            tag,
            minorversion,
            operations: Vec::with_capacity(num_ops),
        };

        for _ in 0..num_ops {
            let op_num = decoder.decode_u32()?;
            let op = Self::decode_operation(decoder, op_num)?;
            request.operations.push(op);
        }

        Ok(request)
    }

    /// Decode a single operation.
    fn decode_operation(decoder: &mut XdrCodec, op_num: u32) -> std::io::Result<super::compound::NfsArgOp4> {
        use super::compound::*;
        use super::types::*;

        match NfsOpNum4::from_u32(op_num) {
            Some(NfsOpNum4::Access) => {
                let access = Access4(decoder.decode_u32()?);
                Ok(NfsArgOp4::Access(access))
            }
            Some(NfsOpNum4::Close) => {
                let seqid = decoder.decode_u32()?;
                let stateid = Self::decode_stateid(decoder)?;
                Ok(NfsArgOp4::Close { seqid, stateid })
            }
            Some(NfsOpNum4::Commit) => {
                let offset = decoder.decode_u64()?;
                let count = decoder.decode_u32()?;
                Ok(NfsArgOp4::Commit { offset, count })
            }
            Some(NfsOpNum4::Getattr) => {
                let attr_request = Self::decode_bitmap(decoder)?;
                Ok(NfsArgOp4::Getattr { attr_request })
            }
            Some(NfsOpNum4::Getfh) => Ok(NfsArgOp4::Getfh),
            Some(NfsOpNum4::Lookup) => {
                let name = decoder.decode_string()?;
                Ok(NfsArgOp4::Lookup { name })
            }
            Some(NfsOpNum4::Lookupp) => Ok(NfsArgOp4::Lookupp),
            Some(NfsOpNum4::Putfh) => {
                let fh_data = decoder.decode_opaque()?;
                Ok(NfsArgOp4::Putfh {
                    fh: NfsFh4(fh_data),
                })
            }
            Some(NfsOpNum4::Putpubfh) => Ok(NfsArgOp4::Putpubfh),
            Some(NfsOpNum4::Putrootfh) => Ok(NfsArgOp4::Putrootfh),
            Some(NfsOpNum4::Read) => {
                let stateid = Self::decode_stateid(decoder)?;
                let offset = decoder.decode_u64()?;
                let count = decoder.decode_u32()?;
                Ok(NfsArgOp4::Read {
                    stateid,
                    offset,
                    count,
                })
            }
            Some(NfsOpNum4::Readdir) => {
                let cookie = decoder.decode_u64()?;
                let cookieverf = Self::decode_verifier(decoder)?;
                let dircount = decoder.decode_u32()?;
                let maxcount = decoder.decode_u32()?;
                let attr_request = Self::decode_bitmap(decoder)?;
                Ok(NfsArgOp4::Readdir {
                    cookie,
                    cookieverf,
                    dircount,
                    maxcount,
                    attr_request,
                })
            }
            Some(NfsOpNum4::Readlink) => Ok(NfsArgOp4::Readlink),
            Some(NfsOpNum4::Remove) => {
                let target = decoder.decode_string()?;
                Ok(NfsArgOp4::Remove { target })
            }
            Some(NfsOpNum4::Rename) => {
                let oldname = decoder.decode_string()?;
                let newname = decoder.decode_string()?;
                Ok(NfsArgOp4::Rename { oldname, newname })
            }
            Some(NfsOpNum4::Restorefh) => Ok(NfsArgOp4::Restorefh),
            Some(NfsOpNum4::Savefh) => Ok(NfsArgOp4::Savefh),
            Some(NfsOpNum4::Setattr) => {
                let stateid = Self::decode_stateid(decoder)?;
                let attrs = Self::decode_fattr4(decoder)?;
                Ok(NfsArgOp4::Setattr { stateid, attrs })
            }
            Some(NfsOpNum4::Write) => {
                let stateid = Self::decode_stateid(decoder)?;
                let offset = decoder.decode_u64()?;
                let stable = decoder.decode_u32()?;
                let data = decoder.decode_opaque()?;
                Ok(NfsArgOp4::Write {
                    stateid,
                    offset,
                    stable,
                    data,
                })
            }
            Some(NfsOpNum4::Sequence) => {
                let session_id = Self::decode_session_id(decoder)?;
                let sequence_id = decoder.decode_u32()?;
                let slot_id = decoder.decode_u32()?;
                let highest_slot_id = decoder.decode_u32()?;
                let cachethis = decoder.decode_bool()?;
                Ok(NfsArgOp4::Sequence {
                    session_id,
                    sequence_id,
                    slot_id,
                    highest_slot_id,
                    cachethis,
                })
            }
            Some(NfsOpNum4::ExchangeId) => {
                let co_ownerid = decoder.decode_opaque()?;
                let co_verifier = Self::decode_verifier(decoder)?;
                let flags = decoder.decode_u32()?;
                let spa_how = decoder.decode_u32()?;
                let num_impl_id = decoder.decode_u32()? as usize;

                let mut impl_id = Vec::with_capacity(num_impl_id);
                for _ in 0..num_impl_id {
                    let domain = decoder.decode_string()?;
                    let name = decoder.decode_string()?;
                    let seconds = decoder.decode_i64()?;
                    let nseconds = decoder.decode_u32()?;
                    impl_id.push(NfsImplId4 {
                        nii_domain: domain,
                        nii_name: name,
                        nii_date: NfsTime4::new(seconds, nseconds),
                    });
                }

                Ok(NfsArgOp4::ExchangeId {
                    client_owner: ClientOwner4 {
                        co_ownerid,
                        co_verifier,
                    },
                    flags,
                    state_protect: StateProtect4 { spa_how },
                    impl_id,
                })
            }
            Some(NfsOpNum4::CreateSession) => {
                let client_id = decoder.decode_u64()?;
                let sequence = decoder.decode_u32()?;
                let flags = decoder.decode_u32()?;
                let fore_chan_attrs = Self::decode_channel_attrs(decoder)?;
                let back_chan_attrs = Self::decode_channel_attrs(decoder)?;
                let cb_program = decoder.decode_u32()?;
                let num_sec_parms = decoder.decode_u32()? as usize;

                let mut sec_parms = Vec::with_capacity(num_sec_parms);
                for _ in 0..num_sec_parms {
                    let flavor = decoder.decode_u32()?;
                    sec_parms.push(CallbackSecParms4 {
                        cb_secflavor: flavor,
                        cb_sys_cred: None,
                    });
                }

                Ok(NfsArgOp4::CreateSession {
                    client_id,
                    sequence,
                    flags,
                    fore_chan_attrs,
                    back_chan_attrs,
                    cb_program,
                    sec_parms,
                })
            }
            Some(NfsOpNum4::DestroySession) => {
                let session_id = Self::decode_session_id(decoder)?;
                Ok(NfsArgOp4::DestroySession { session_id })
            }
            Some(NfsOpNum4::ReclaimComplete) => {
                let one_fs = decoder.decode_bool()?;
                Ok(NfsArgOp4::ReclaimComplete { one_fs })
            }
            _ => {
                // Unknown or unimplemented operation
                Ok(NfsArgOp4::Illegal)
            }
        }
    }

    fn decode_stateid(decoder: &mut XdrCodec) -> std::io::Result<Stateid4> {
        let seqid = decoder.decode_u32()?;
        let other_data = decoder.decode_opaque_fixed(12)?;
        let mut other = [0u8; 12];
        other.copy_from_slice(&other_data);
        Ok(Stateid4 { seqid, other })
    }

    fn decode_verifier(decoder: &mut XdrCodec) -> std::io::Result<Verifier4> {
        let data = decoder.decode_opaque_fixed(8)?;
        let mut arr = [0u8; 8];
        arr.copy_from_slice(&data);
        Ok(Verifier4(arr))
    }

    fn decode_bitmap(decoder: &mut XdrCodec) -> std::io::Result<AttrBitmap> {
        let words = decoder.decode_u32_array()?;
        Ok(AttrBitmap(words))
    }

    fn decode_fattr4(decoder: &mut XdrCodec) -> std::io::Result<Attr4> {
        let attrmask = Self::decode_bitmap(decoder)?;
        let attr_vals = decoder.decode_opaque()?;
        Ok(Attr4 {
            attrmask,
            attr_vals,
        })
    }

    fn decode_session_id(decoder: &mut XdrCodec) -> std::io::Result<super::session::NfsSessionId> {
        let data = decoder.decode_opaque_fixed(16)?;
        let mut arr = [0u8; 16];
        arr.copy_from_slice(&data);
        Ok(super::session::NfsSessionId(arr))
    }

    fn decode_channel_attrs(decoder: &mut XdrCodec) -> std::io::Result<super::compound::ChannelAttrs4> {
        Ok(super::compound::ChannelAttrs4 {
            ca_headerpadsize: decoder.decode_u32()?,
            ca_maxrequestsize: decoder.decode_u32()?,
            ca_maxresponsesize: decoder.decode_u32()?,
            ca_maxresponsesize_cached: decoder.decode_u32()?,
            ca_maxoperations: decoder.decode_u32()?,
            ca_maxrequests: decoder.decode_u32()?,
            ca_rdma_ird: if decoder.decode_bool()? {
                Some(decoder.decode_u32()?)
            } else {
                None
            },
        })
    }

    /// Encode a COMPOUND response.
    fn encode_compound_response(response: &super::compound::CompoundResponse) -> Vec<u8> {
        let mut encoder = XdrCodec::new();

        // Status
        encoder.encode_u32(response.status.to_u32());
        // Tag
        encoder.encode_string(&response.tag);
        // Number of results
        encoder.encode_u32(response.results.len() as u32);

        // Encode each result
        for result in &response.results {
            Self::encode_operation_result(&mut encoder, result);
        }

        encoder.into_bytes()
    }

    /// Encode a single operation result.
    fn encode_operation_result(encoder: &mut XdrCodec, result: &super::compound::NfsResOp4) {
        use super::compound::*;

        // Operation number
        encoder.encode_u32(result.op as u32);
        // Status
        encoder.encode_u32(result.status.to_u32());

        // Result-specific data (only if success for most operations)
        if result.status == super::error::NfsStatus::Ok {
            match &result.result {
                OpResult::None => {}
                OpResult::Access { supported, access } => {
                    encoder.encode_u32(supported.0);
                    encoder.encode_u32(access.0);
                }
                OpResult::Close { stateid } => {
                    Self::encode_stateid(encoder, stateid);
                }
                OpResult::Commit { verifier } => {
                    encoder.encode_opaque_fixed(&verifier.0);
                }
                OpResult::Create { cinfo, attrs } => {
                    Self::encode_change_info(encoder, cinfo);
                    Self::encode_bitmap(encoder, attrs);
                }
                OpResult::Getattr { attrs } => {
                    Self::encode_bitmap(encoder, &attrs.attrmask);
                    encoder.encode_opaque(&attrs.attr_vals);
                }
                OpResult::Getfh { fh } => {
                    encoder.encode_opaque(&fh.0);
                }
                OpResult::Link { cinfo } => {
                    Self::encode_change_info(encoder, cinfo);
                }
                OpResult::Lock { stateid } => {
                    Self::encode_stateid(encoder, stateid);
                }
                OpResult::Locku { stateid } => {
                    Self::encode_stateid(encoder, stateid);
                }
                OpResult::Open {
                    stateid,
                    cinfo,
                    rflags,
                    attrset,
                    delegation,
                } => {
                    Self::encode_stateid(encoder, stateid);
                    Self::encode_change_info(encoder, cinfo);
                    encoder.encode_u32(*rflags);
                    Self::encode_bitmap(encoder, attrset);
                    // Encode delegation
                    match delegation {
                        OpenDelegation4::None => encoder.encode_u32(0),
                        OpenDelegation4::Read { .. } => encoder.encode_u32(1),
                        OpenDelegation4::Write { .. } => encoder.encode_u32(2),
                    }
                }
                OpResult::Read { eof, data } => {
                    encoder.encode_bool(*eof);
                    encoder.encode_opaque(data);
                }
                OpResult::Readdir {
                    cookieverf,
                    entries,
                    eof,
                } => {
                    encoder.encode_opaque_fixed(&cookieverf.0);
                    // Encode directory entries
                    for entry in entries {
                        encoder.encode_bool(true); // entry follows
                        encoder.encode_u64(entry.cookie);
                        encoder.encode_string(&entry.name);
                        Self::encode_bitmap(encoder, &entry.attrs.attrmask);
                        encoder.encode_opaque(&entry.attrs.attr_vals);
                    }
                    encoder.encode_bool(false); // no more entries
                    encoder.encode_bool(*eof);
                }
                OpResult::Readlink { link } => {
                    encoder.encode_string(link);
                }
                OpResult::Remove { cinfo } => {
                    Self::encode_change_info(encoder, cinfo);
                }
                OpResult::Rename {
                    source_cinfo,
                    target_cinfo,
                } => {
                    Self::encode_change_info(encoder, source_cinfo);
                    Self::encode_change_info(encoder, target_cinfo);
                }
                OpResult::Setattr { attrsset } => {
                    Self::encode_bitmap(encoder, attrsset);
                }
                OpResult::Write {
                    count,
                    committed,
                    verifier,
                } => {
                    encoder.encode_u32(*count);
                    encoder.encode_u32(*committed);
                    encoder.encode_opaque_fixed(&verifier.0);
                }
                OpResult::ExchangeId {
                    client_id,
                    sequence_id,
                    flags,
                    state_protect,
                    server_owner,
                    server_scope,
                    server_impl_id,
                } => {
                    encoder.encode_u64(*client_id);
                    encoder.encode_u32(*sequence_id);
                    encoder.encode_u32(*flags);
                    encoder.encode_u32(state_protect.spa_how);
                    encoder.encode_u64(server_owner.so_minor_id);
                    encoder.encode_opaque(&server_owner.so_major_id);
                    encoder.encode_opaque(server_scope);
                    encoder.encode_u32(server_impl_id.len() as u32);
                    for impl_id in server_impl_id {
                        encoder.encode_string(&impl_id.nii_domain);
                        encoder.encode_string(&impl_id.nii_name);
                        encoder.encode_i64(impl_id.nii_date.seconds);
                        encoder.encode_u32(impl_id.nii_date.nseconds);
                    }
                }
                OpResult::CreateSession {
                    session_id,
                    sequence_id,
                    flags,
                    fore_chan_attrs,
                    back_chan_attrs,
                } => {
                    encoder.encode_opaque_fixed(&session_id.0);
                    encoder.encode_u32(*sequence_id);
                    encoder.encode_u32(*flags);
                    Self::encode_channel_attrs(encoder, fore_chan_attrs);
                    Self::encode_channel_attrs(encoder, back_chan_attrs);
                }
                OpResult::Sequence {
                    session_id,
                    sequence_id,
                    slot_id,
                    highest_slot_id,
                    target_highest_slot_id,
                    status_flags,
                } => {
                    encoder.encode_opaque_fixed(&session_id.0);
                    encoder.encode_u32(*sequence_id);
                    encoder.encode_u32(*slot_id);
                    encoder.encode_u32(*highest_slot_id);
                    encoder.encode_u32(*target_highest_slot_id);
                    encoder.encode_u32(*status_flags);
                }
                _ => {}
            }
        }
    }

    fn encode_stateid(encoder: &mut XdrCodec, stateid: &Stateid4) {
        encoder.encode_u32(stateid.seqid);
        encoder.encode_opaque_fixed(&stateid.other);
    }

    fn encode_change_info(encoder: &mut XdrCodec, cinfo: &ChangeInfo4) {
        encoder.encode_bool(cinfo.atomic);
        encoder.encode_u64(cinfo.before);
        encoder.encode_u64(cinfo.after);
    }

    fn encode_bitmap(encoder: &mut XdrCodec, bitmap: &AttrBitmap) {
        encoder.encode_u32_array(&bitmap.0);
    }

    fn encode_channel_attrs(encoder: &mut XdrCodec, attrs: &super::compound::ChannelAttrs4) {
        encoder.encode_u32(attrs.ca_headerpadsize);
        encoder.encode_u32(attrs.ca_maxrequestsize);
        encoder.encode_u32(attrs.ca_maxresponsesize);
        encoder.encode_u32(attrs.ca_maxresponsesize_cached);
        encoder.encode_u32(attrs.ca_maxoperations);
        encoder.encode_u32(attrs.ca_maxrequests);
        encoder.encode_bool(attrs.ca_rdma_ird.is_some());
        if let Some(ird) = attrs.ca_rdma_ird {
            encoder.encode_u32(ird);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_server_config_default() {
        let config = NfsServerConfig::default();
        assert_eq!(config.bind_addr.port(), 2049);
        assert_eq!(config.max_connections, 1000);
    }
}
