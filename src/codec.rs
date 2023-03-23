use bytes::{Buf, BufMut, BytesMut};
use lazy_static::lazy_static;
use num_derive::{FromPrimitive, ToPrimitive};
use num_traits::FromPrimitive;
use tokio_util::codec::{Decoder, Encoder, LengthDelimitedCodec};

use crate::constants::*;
use crate::errors::ZkError;
use crate::record::{Deserialize, Record};

#[derive(Debug)]
pub struct ConnectRequest {
    pub protocol_version: i32,
    pub last_zxid_seen: i64,
    pub timeout: i32,
    pub session_id: i64,
    pub passwd: Vec<u8>,
    pub read_only: bool,
}

impl ConnectRequest {}

impl Deserialize for ConnectRequest {
    fn deserialize(bytes: &mut bytes::BytesMut) -> Result<Self, ZkError> {
        let last_zxid_seen = bytes.get_i64();
        let timeout = bytes.get_i32();
        let session_id = bytes.get_i64();
        let passwd_len = bytes.get_i32();
        let passwd = if passwd_len > 0 {
            let mut passwd = vec![0; passwd_len as usize];
            bytes.copy_to_slice(&mut passwd);
            passwd
        } else {
            vec![]
        };
        let read_only = maybe_read_bool(bytes);
        Ok(ConnectRequest {
            protocol_version: 0,
            last_zxid_seen,
            timeout,
            session_id,
            passwd,
            read_only,
        })
    }
}

impl Record for ConnectRequest {
    fn serialize_into(&self, buffer: &mut bytes::BytesMut) -> Result<(), ZkError> {
        buffer.put_i32(self.protocol_version);
        buffer.put_i64(self.last_zxid_seen);
        buffer.put_i32(self.timeout);
        buffer.put_i64(self.session_id);
        buffer.put_i32(self.passwd.len() as i32);
        buffer.extend_from_slice(&self.passwd);
        buffer.put_u8(self.read_only as u8);
        Ok(())
    }

    fn size(&self) -> usize {
        4 + 8 + 4 + 8 + 4 + self.passwd.len() + 1
    }
}


#[derive(Debug)]
pub struct PingRequest;

impl Deserialize for PingRequest {
    fn deserialize(bytes: &mut bytes::BytesMut) -> Result<Self, ZkError> {
        Ok(PingRequest {})
    }
}

impl Record for PingRequest {
    fn serialize_into(&self, _buffer: &mut bytes::BytesMut) -> Result<(), ZkError> {
        Ok(())
    }
    fn size(&self) -> usize {
        0
    }
}

#[derive(Debug)]
pub struct GetDataRequest {
    pub path: String,
    pub watch: bool,
}

impl Deserialize for GetDataRequest {
    fn deserialize(bytes: &mut bytes::BytesMut) -> Result<Self, ZkError> {
        let path = get_str(bytes)?;
        let watch = bytes.get_u8() == 1;
        Ok(GetDataRequest { path, watch })
    }
}

impl Record for GetDataRequest {
    fn serialize_into(&self, buffer: &mut bytes::BytesMut) -> Result<(), ZkError> {
        buffer.put_i32(self.path.len() as i32);
        buffer.extend_from_slice(self.path.as_bytes());
        buffer.put_u8(self.watch as u8);
        Ok(())
    }

    fn size(&self) -> usize {
        4 + self.path.len() + 1
    }
}

#[derive(Debug)]
pub struct Acl {
    pub perms: i32,
    pub scheme: Vec<u8>,
    pub cred: Vec<u8>,
}

#[derive(Debug)]
pub struct CreateRequest {
    pub path: String,
    pub data: Vec<u8>,
    pub acl: Vec<Acl>,
    pub flags: i32,
}

#[derive(Debug)]
pub struct SetDataRequest {
    path: String,
    data: Vec<u8>,
    version: i32,
}

impl Deserialize for SetDataRequest {
    fn deserialize(bytes: &mut BytesMut) -> Result<Self, ZkError> {
        let path = get_str(bytes)?;
        let data = get_data(bytes)?;
        let version = bytes.get_i32();
        Ok(SetDataRequest { path, data, version })
    }
}

impl Record for SetDataRequest {
    fn serialize_into(&self, buffer: &mut BytesMut) -> Result<(), ZkError> {
        buffer.put_i32(self.path.len() as i32);
        buffer.extend_from_slice(self.path.as_bytes());
        buffer.put_i32(self.data.len() as i32);
        buffer.extend_from_slice(&self.data);
        buffer.put_i32(self.version);
        Ok(())
    }

    fn size(&self) -> usize {
        4 + self.path.len() + 4 + self.data.len() + 4
    }
}


impl Deserialize for CreateRequest {
    fn deserialize(bytes: &mut bytes::BytesMut) -> Result<Self, ZkError> {
        let path = get_str(bytes)?;
        let data = get_data(bytes)?;
        let acl = get_acl(bytes)?;
        let flags = bytes.get_i32();
        let req = CreateRequest {
            path,
            data,
            acl,
            flags,
        };
        Ok(req)
    }
}

impl Record for CreateRequest {
    fn serialize_into(&self, buffer: &mut bytes::BytesMut) -> Result<(), ZkError> {
        buffer.put_i32(self.path.len() as i32);
        buffer.extend_from_slice(self.path.as_bytes());
        buffer.put_i32(self.data.len() as i32);
        buffer.extend_from_slice(&self.data);
        buffer.put_i32(self.acl.len() as i32);
        for acl in &self.acl {
            buffer.put_i32(acl.perms);
            buffer.put_i32(acl.scheme.len() as i32);
            buffer.extend_from_slice(&acl.scheme);
            buffer.put_i32(acl.cred.len() as i32);
            buffer.extend_from_slice(&acl.cred);
        }
        buffer.put_i32(self.flags);
        Ok(())
    }

    fn size(&self) -> usize {
        4 + self.path.len() + 4 + self.data.len() + 4 + self.acl.iter().map(|a| 4 + 4 + a.scheme.len() + 4 + a.cred.len()).sum::<usize>() + 4
    }
}

#[derive(Debug)]
pub struct DeleteRequest {
    pub path: String,
    pub version: i32,
}

impl Deserialize for DeleteRequest {
    fn deserialize(bytes: &mut bytes::BytesMut) -> Result<Self, ZkError> {
        let path = get_str(bytes)?;
        let version = bytes.get_i32();
        let req = DeleteRequest {
            path,
            version,
        };
        Ok(req)
    }
}

impl Record for DeleteRequest {
    fn serialize_into(&self, buffer: &mut bytes::BytesMut) -> Result<(), ZkError> {
        buffer.put_i32(self.path.len() as i32);
        buffer.extend_from_slice(self.path.as_bytes());
        buffer.put_i32(self.version);
        Ok(())
    }

    fn size(&self) -> usize {
        4 + self.path.len() + 4
    }
}

#[derive(Debug)]
pub struct GetChildrenRequest {
    pub path: String,
    pub watch: bool,
}

impl Record for GetChildrenRequest {
    fn serialize_into(&self, buffer: &mut BytesMut) -> Result<(), ZkError> {
        buffer.put_i32(self.path.len() as i32);
        buffer.extend_from_slice(self.path.as_bytes());
        buffer.put_u8(self.watch as u8);
        Ok(())
    }

    fn size(&self) -> usize {
        4 + self.path.len() + 1
    }
}

impl Deserialize for GetChildrenRequest {
    fn deserialize(bytes: &mut BytesMut) -> Result<Self, ZkError> {
        let path = get_str(bytes)?;
        let watch = bytes.get_u8();
        let req = GetChildrenRequest {
            path,
            watch: watch != 0,
        };
        Ok(req)
    }
}

#[derive(Debug)]
pub struct RequestPacket {
    pub request_header: Option<RequestHeader>,
    pub request: Box<dyn Record>,
}

impl Record for RequestPacket {
    fn serialize_into(&self, buffer: &mut bytes::BytesMut) -> Result<(), ZkError> {
        if let Some(header) = &self.request_header {
            header.serialize_into(buffer)?;
        }
        self.request.serialize_into(buffer)?;
        Ok(())
    }

    fn size(&self) -> usize {
        self.request_header.as_ref().map_or(0, |h| h.size()) + self.request.size()
    }
}

#[derive(Debug)]
pub struct RequestHeader {
    pub xid: i32,
    pub opcode: i32,
}

impl Record for RequestHeader {
    fn serialize_into(&self, buffer: &mut bytes::BytesMut) -> Result<(), ZkError> {
        buffer.put_i32(self.xid);
        buffer.put_i32(self.opcode);
        Ok(())
    }

    fn size(&self) -> usize {
        8
    }
}

#[derive(Debug)]
pub enum ZkResponse {
    Connect(ConnectResponse),
    GetData(GetDataResponse),
    Ping(ReplyHeader),
}

impl Record for ZkResponse {
    fn serialize_into(&self, buffer: &mut bytes::BytesMut) -> Result<(), ZkError> {
        match self {
            ZkResponse::Connect(resp) => {
                buffer.put_i32(resp.protocol_version);
                buffer.put_i32(resp.timeout);
                buffer.put_i64(resp.session_id);
                buffer.put_i32(resp.passwd.len() as i32);
                buffer.extend_from_slice(&resp.passwd);
                buffer.put_i8(resp.read_only as i8);
            }
            ZkResponse::GetData(r) => {
                r.reply_header.serialize_into(buffer)?;
                buffer.put_i32(r.data.len() as i32);
                buffer.extend_from_slice(&r.data);
                buffer.put_i64(r.czxid);
                buffer.put_i64(r.mzxid);
                buffer.put_i64(r.ctime);
                buffer.put_i64(r.mtime);
                buffer.put_i32(r.version);
                buffer.put_i32(r.cversion);
                buffer.put_i32(r.aversion);
                buffer.put_i64(r.ephemeral_owner);
                buffer.put_i32(r.data_length);
                buffer.put_i32(r.num_children);
                buffer.put_i64(r.pzxid);
            }
            ZkResponse::Ping(resp) => {
                resp.serialize_into(buffer)?;
            }
        }
        Ok(())
    }

    fn size(&self) -> usize {
        match self {
            ZkResponse::Connect(r) => {
                4 + 4 + 8 + 4 + r.passwd.len() + 1
            }
            ZkResponse::GetData(r) => {
                r.reply_header.size() + 4 + r.data.len() + 6 * 8 + 4 * 5
            }
            ZkResponse::Ping(r) => {
                r.size()
            }
        }
    }
}

#[derive(Debug)]
pub struct ReplyHeader {
    pub xid: i32,
    pub zxid: i64,
    pub err: i32,
}

impl Record for ReplyHeader {
    fn serialize_into(&self, buffer: &mut bytes::BytesMut) -> Result<(), ZkError> {
        buffer.put_i32(self.xid);
        buffer.put_i64(self.zxid);
        buffer.put_i32(self.err);
        Ok(())
    }

    fn size(&self) -> usize {
        16
    }
}

#[derive(Debug)]
pub struct ConnectResponse {
    pub protocol_version: i32,
    pub timeout: i32,
    pub session_id: i64,
    pub passwd: Vec<u8>,
    pub read_only: bool,
}

#[derive(Debug)]
pub struct GetDataResponse {
    pub reply_header: ReplyHeader,
    pub data: Vec<u8>,

    pub czxid: i64,
    /// The last transaction that modified the znode.
    pub mzxid: i64,
    /// Milliseconds since epoch when the znode was created.
    pub ctime: i64,
    /// Milliseconds since epoch when the znode was last modified.
    pub mtime: i64,
    /// The number of changes to the data of the znode.
    pub version: i32,
    /// The number of changes to the children of the znode.
    pub cversion: i32,
    /// The number of changes to the ACL of the znode.
    pub aversion: i32,
    /// The session ID of the owner of this znode, if it is an ephemeral entry.
    pub ephemeral_owner: i64,
    /// The length of the data field of the znode.
    pub data_length: i32,
    /// The number of children this znode has.
    pub num_children: i32,
    /// The transaction ID that last modified the children of the znode.
    pub pzxid: i64,

}

impl GetDataResponse {
    fn serialize_into(&self, buffer: &mut bytes::BytesMut) -> Result<(), ZkError> {
        self.reply_header.serialize_into(buffer)?;
        buffer.put_i32(self.data.len() as i32);
        buffer.extend_from_slice(&self.data);

        buffer.put_i64(self.czxid);
        buffer.put_i64(self.mzxid);
        buffer.put_i64(self.ctime);
        buffer.put_i64(self.mtime);
        buffer.put_i32(self.version);
        buffer.put_i32(self.cversion);
        buffer.put_i32(self.aversion);
        buffer.put_i64(self.ephemeral_owner);
        buffer.put_i32(self.data_length);
        buffer.put_i32(self.num_children);
        buffer.put_i64(self.pzxid);
        Ok(())
    }
}

#[derive(Debug, FromPrimitive, ToPrimitive)]
pub enum XidCodes {
    ConnectXid = 0,
    WatchXid = -1,
    PingXid = -2,
    AuthXid = -4,
    SetWatchesXid = -8,
}

lazy_static! {
    static ref LENGTH_DELIMITED_CODEC: LengthDelimitedCodec
        = LengthDelimitedCodec::builder()
        .max_frame_length(1 * 1_024 * 1_024)
        .length_field_length(4)
        .length_field_offset(0)
        .length_adjustment(0)
        .big_endian()
        .new_codec();
}

pub struct ClientPacketCodec {}

impl ClientPacketCodec {
    pub fn new() -> Self {
        Self {}
    }
}

impl Encoder<RequestPacket> for ClientPacketCodec {
    type Error = ZkError;

    fn encode(&mut self, item: RequestPacket, dst: &mut bytes::BytesMut) -> Result<(), Self::Error> {
        let n = item.size();
        dst.reserve(n + 4);
        dst.put_i32(n as i32);
        item.serialize_into(dst)?;
        Ok(())
    }
}

pub struct ServerPacketCodec {
    inner: LengthDelimitedCodec,
}

impl ServerPacketCodec {
    pub fn new() -> Self {
        Self {
            inner: LENGTH_DELIMITED_CODEC.clone()
        }
    }
    fn parse_connect(&self, bytes: &mut bytes::BytesMut) -> Result<ConnectRequest, ZkError> {
        let connect_req = {
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
        Ok(connect_req)
    }
    fn decode_inner(src: &mut bytes::BytesMut) -> Result<Option<RequestPacket>, ZkError> {
        let xid = src.get_i32();
        let xid_enum = XidCodes::from_i32(xid);

        println!("xid:{} xid: {:?}", xid, xid_enum);
        if let Some(xid_enum) = xid_enum {
            match xid_enum {
                XidCodes::ConnectXid => {
                    let req = ConnectRequest::deserialize(src)?;
                    println!("connect req: {:?}", req);
                    return Ok(Some(RequestPacket {
                        request_header: None,
                        request: Box::new(req),
                    }));
                }
                XidCodes::WatchXid => {}
                XidCodes::PingXid => {
                    let opcode = src.get_i32();
                    return Ok(Some(RequestPacket {
                        request_header: Some(RequestHeader { xid, opcode }),
                        request: Box::new(PingRequest {}),
                    }));
                }
                XidCodes::AuthXid => {}
                XidCodes::SetWatchesXid => {}
            }
        }
        // Data requests, with XIDs > 0.

        let opcode = src.get_i32();
        let opcode_enum = OpCodes::from_i32(opcode).unwrap();
        println!("opcode_enum: {:?}", opcode_enum);
        match opcode_enum {
            OpCodes::GetData => {
                let req = GetDataRequest::deserialize(src)?;
                return Ok(Some(
                    RequestPacket {
                        request_header: Some(RequestHeader { xid, opcode }),
                        request: Box::new(req),
                    }));
            }
            OpCodes::Create | OpCodes::Create2 | OpCodes::CreateTtl | OpCodes::CreateContainer => {
                let req = CreateRequest::deserialize(src)?;
                return Ok(Some(
                    RequestPacket {
                        request_header: Some(RequestHeader { xid, opcode }),
                        request: Box::new(req),
                    }));
            }
            OpCodes::SetData => {
                let req = SetDataRequest::deserialize(src)?;
                return Ok(Some(
                    RequestPacket {
                        request_header: Some(RequestHeader { xid, opcode }),
                        request: Box::new(req),
                    }));
            }
            OpCodes::Delete => {
                let req = DeleteRequest::deserialize(src)?;
                return Ok(Some(
                    RequestPacket {
                        request_header: Some(RequestHeader { xid, opcode }),
                        request: Box::new(req),
                    }));
            }
            OpCodes::GetChildren | OpCodes::GetChildren2 => {
                let req = GetChildrenRequest::deserialize(src)?;
                return Ok(Some(
                    RequestPacket {
                        request_header: Some(RequestHeader { xid, opcode }),
                        request: Box::new(req),
                    }));
            }
            OpCodes::Exists => {}
            OpCodes::GetAcl => {}
            OpCodes::SetAcl => {}
            OpCodes::Sync => {}
            OpCodes::Check => {}
            OpCodes::Multi => {}
            OpCodes::Reconfig => {}
            OpCodes::CheckWatches => {}
            OpCodes::RemoveWatches => {}
            OpCodes::Close => {
                return Ok(Some(RequestPacket {
                    request_header: Some(RequestHeader { xid, opcode }),
                    request: Box::new(PingRequest {}),
                }));
            }
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

impl Decoder for ServerPacketCodec {
    type Item = RequestPacket;
    type Error = ZkError;

    fn decode(&mut self, src: &mut bytes::BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        return match self.inner.decode(src) {
            Err(_) => {
                Err(ZkError::DecodeError)
            }
            Ok(res) => {
                match res {
                    None => {
                        Ok(None)
                    }
                    Some(mut src) => {
                        Self::decode_inner(&mut src)
                    }
                }
            }
        };
    }
}

fn ensure_min_length(len: i32, min: i32) -> Result<(), ZkError> {
    if len < min {
        return Err(ZkError::InvalidPacketLength(len));
    }
    Ok(())
}

fn ensure_max_len(len: usize) -> Result<(), ZkError> {
    // if len > MAX_PACKET_LENGTH {
    //     return Err(ZkError::InvalidPacketLength(len));
    // }
    Ok(())
}

fn get_acl(bytes: &mut bytes::BytesMut) -> Result<Vec<Acl>, ZkError> {
    let len = bytes.get_i32();
    if len <= 0 {
        return Ok(vec![]);
    }
    if bytes.len() < len as usize {
        return Err(ZkError::InvalidPacketLength(len));
    }
    ensure_max_len(len as usize)?;
    let mut vec = vec![];
    for _ in 0..len {
        let perms = bytes.get_i32();
        let scheme = get_data(bytes)?;
        let cred = get_data(bytes)?;
        vec.push(Acl {
            perms,
            scheme,
            cred,
        });
    }
    Ok(vec)
}

fn get_data(bytes: &mut bytes::BytesMut) -> Result<Vec<u8>, ZkError> {
    let len = bytes.get_i32();
    println!("get data: len: {}", len);
    if len <= 0 {
        return Ok(vec![]);
    }
    if bytes.len() < len as usize {
        return Err(ZkError::InvalidPacketLength(len));
    }
    ensure_max_len(len as usize)?;
    let mut vec = vec![0; len as usize];
    bytes.copy_to_slice(&mut vec);
    Ok(vec)
}

fn get_str(bytes: &mut bytes::BytesMut) -> Result<String, ZkError> {
    let len = bytes.get_i32();
    if len <= 0 {
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

fn maybe_read_bool(bytes: &mut bytes::BytesMut) -> bool {
    return if bytes.remaining() >= 1 {
        bytes.get_u8() == 1
    } else {
        false
    };
}
