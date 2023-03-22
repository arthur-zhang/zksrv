use bytes::{Buf, BufMut};
use num_derive::{FromPrimitive, ToPrimitive};
use num_traits::{FromPrimitive, ToPrimitive};
use tokio_util::codec::{Decoder, Encoder};

use crate::constants::*;
use crate::errors::ZkError;
use crate::proto::{ConnectRequest, GetDataRequest, GetDataResponse, ReplyHeader, RequestHeader, RequestPacket, ZkRequest, ZkResponse};
use crate::record::Record;

pub fn maybe_read_bool(bytes: &mut bytes::BytesMut) -> bool {
    return if bytes.remaining() >= 1 {
        bytes.get_u8() == 1
    } else {
        false
    };
}

#[derive(Debug, FromPrimitive, ToPrimitive)]
pub enum XidCodes {
    ConnectXid = 0,
    WatchXid = -1,
    PingXid = -2,
    AuthXid = -4,
    SetWatchesXid = -8,
}


pub struct ClientPacketCodec;


impl ClientPacketCodec {
    pub fn new() -> Self {
        Self {}
    }
}

pub fn ensure_min_length(len: i32, min: i32) -> Result<(), ZkError> {
    if len < min {
        return Err(ZkError::InvalidPacketLength(len));
    }
    Ok(())
}

impl Encoder<RequestPacket> for ClientPacketCodec {
    type Error = ZkError;

    fn encode(&mut self, item: RequestPacket, dst: &mut bytes::BytesMut) -> Result<(), Self::Error> {
        println!("size: {}", item.size());
        dst.put_i32(item.size() as i32);
        item.serialize_into(dst)?;
        Ok(())
    }
}

impl Decoder for ClientPacketCodec {
    type Item = ZkResponse;
    type Error = ZkError;

    fn decode(&mut self, src: &mut bytes::BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if src.len() < 4 {
            return Ok(None);
        }
        let packet_len = src.get_i32();
        println!("packet len: {}", packet_len);
        if packet_len == 0 {
            return Ok(None);
        }

        let xid = src.get_i32();
        println!("xid: {}", xid);
        let zxid = src.get_i64();
        let err = src.get_i32();
        match xid {
            -2 => {
                return Ok(Some(ZkResponse::Ping({
                    ReplyHeader {
                        xid,
                        zxid,
                        err,
                    }
                })));
            }
            _ => {}
        }
        if err != 0 {
            return Ok(Some(ZkResponse::Ping({
                ReplyHeader {
                    xid,
                    zxid,
                    err,
                }
            })));
        }

        println!("err: {}", err);
        let len = src.get_i32();
        println!("len: {}", len);

        let mut buf: Vec<u8>;
        if len > 0 {
            buf = vec![0; len as usize];
            src.copy_to_slice(&mut buf);
        } else {
            buf = vec![];
        }
        Ok(Some(ZkResponse::GetData(GetDataResponse {
            reply_header: ReplyHeader {
                xid,
                zxid,
                err,
            },
            data: buf,
            czxid: src.get_i64(),
            mzxid: src.get_i64(),
            ctime: src.get_i64(),
            mtime: src.get_i64(),
            version: src.get_i32(),
            cversion: src.get_i32(),
            aversion: src.get_i32(),
            ephemeral_owner: src.get_i64(),
            data_length: src.get_i32(),
            num_children: src.get_i32(),
            pzxid: src.get_i64(),
        })))
    }
}

pub struct ServerPacketCodec {}

impl ServerPacketCodec {
    pub fn new() -> Self {
        Self {}
    }
    fn parse_connect(&self, bytes: &mut bytes::BytesMut) -> Result<ZkRequest, ZkError> {
        let connect_req = {
            // ensure_min_length(len as i32, XID_LENGTH + ZXID_LENGTH + TIMEOUT_LENGTH + SESSION_LENGTH + INT_LENGTH)?;
            // let protocol_version = bytes.get_i32();
            let last_zxid_seen = bytes.get_i64();
            let timeout = bytes.get_i32();
            let session_id = bytes.get_i64();
            let passwd_len = bytes.get_i32();
            let mut passwd = vec![0; passwd_len as usize];
            bytes.copy_to_slice(&mut passwd);
            let read_only = maybe_read_bool(bytes);
            ConnectRequest {
                protocol_version: 0,
                last_zxid_seen,
                timeout,
                session_id,
                passwd,
                read_only,
            }
        };
        Ok(ZkRequest::ConnectInit(connect_req))
    }
}

impl Encoder<ZkResponse> for ServerPacketCodec {
    type Error = ZkError;

    fn encode(&mut self, item: ZkResponse, dst: &mut bytes::BytesMut) -> Result<(), Self::Error> {
        dst.put_i32(item.size() as i32);
        item.serialize_into(dst);
        Ok(())
    }
}

fn ensure_max_len(len: usize) -> Result<(), ZkError> {
    // if len > MAX_PACKET_LENGTH {
    //     return Err(ZkError::InvalidPacketLength(len));
    // }
    Ok(())
}

fn get_str(bytes: &mut bytes::BytesMut) -> Result<String, ZkError> {
    let len = bytes.get_i32();
    if len == 0 {
        return Ok("".to_string());
    }
    if bytes.len() < len as usize {
        return Err(ZkError::InvalidPacketLength(len));
    }
    ensure_max_len(len as usize)?;
    let mut vec = vec![0; len as usize];
    bytes.copy_to_slice(&mut vec);
    let str = String::from_utf8(vec).map_err(|e| ZkError::InvalidString)?;
    Ok(str)
}

impl Decoder for ServerPacketCodec {
    type Item = RequestPacket;
    type Error = ZkError;

    fn decode(&mut self, src: &mut bytes::BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if src.len() < 4 {
            return Ok(None);
        }

        let packet_len = src.get_i32();
        ensure_min_length(packet_len, XID_LENGTH + INT_LENGTH)?; // xid + opcode
        ensure_max_len(packet_len as usize)?;

        let xid = src.get_i32();
        println!("packet_len:{} xid: {:?}", packet_len, xid);
        let xid_enum = XidCodes::from_i32(xid);
        // println!("packet_len:{} xid: {:?}", packet_len, xid_enum);
        if let Some(xid_enum) = xid_enum {
            match xid_enum {
                XidCodes::ConnectXid => {
                    let req = self.parse_connect(src)?; // todo
                    return Ok(Some(RequestPacket {
                        request_header: None,
                        request: req,
                    }));
                }
                XidCodes::WatchXid => {}
                XidCodes::PingXid => {
                    let opcode = src.get_i32();
                    return Ok(Some(RequestPacket {
                        request_header: Some(RequestHeader { xid, opcode }),
                        request: ZkRequest::Ping,
                    }));
                }
                XidCodes::AuthXid => {}
                XidCodes::SetWatchesXid => {}
            }
        }
        // Data requests, with XIDs > 0.

        let opcode = src.get_i32();
        let opcode_enum = OpCodes::from_i32(opcode).unwrap();
        println!("{:?}", opcode_enum);
        match opcode_enum {
            OpCodes::GetData => {
                let path = get_str(src)?;
                let watch = src.get_u8() == 1;
                let req = GetDataRequest {
                    path,
                    watch,
                };
                return Ok(Some(
                    RequestPacket {
                        request_header: Some(RequestHeader { xid, opcode }),
                        request: ZkRequest::GetData(req),
                    }));
            }
            OpCodes::Create => {}
            OpCodes::Delete => {}
            OpCodes::Exists => {}
            OpCodes::SetData => {}
            OpCodes::GetAcl => {}
            OpCodes::SetAcl => {}
            OpCodes::GetChildren => {}
            OpCodes::Sync => {}
            OpCodes::GetChildren2 => {}
            OpCodes::Check => {}
            OpCodes::Multi => {}
            OpCodes::Create2 => {}
            OpCodes::Reconfig => {}
            OpCodes::CheckWatches => {}
            OpCodes::RemoveWatches => {}
            OpCodes::CreateContainer => {}
            OpCodes::CreateTtl => {}
            OpCodes::Close => {}
            OpCodes::SetAuth => {}
            OpCodes::SetWatches => {}
            OpCodes::GetEphemerals => {}
            OpCodes::GetAllChildrenNumber => {}
            OpCodes::SetWatches2 => {}
            _ => {}
        }
        unreachable!()
    }
}
