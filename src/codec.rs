use std::collections::HashMap;
use std::os::macos::raw::stat;
use std::sync::Arc;
use bytes::{Buf, BufMut, BytesMut};
use dashmap::DashMap;
use lazy_static::lazy_static;
use num_derive::{FromPrimitive, ToPrimitive};
use num_traits::{FromPrimitive, ToPrimitive};
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

impl Record for Acl {
    fn serialize_into(&self, buffer: &mut BytesMut) -> Result<(), ZkError> {
        buffer.put_i32(self.perms);
        buffer.put_i32(self.scheme.len() as i32);
        buffer.extend_from_slice(&self.scheme);
        buffer.put_i32(self.cred.len() as i32);
        buffer.extend_from_slice(&self.cred);
        Ok(())
    }

    fn size(&self) -> usize {
        4 + 4 + self.scheme.len() + 4 + self.cred.len()
    }
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
pub struct ExistsRequest {
    pub path: String,
    pub watch: bool,
}

impl Record for ExistsRequest {
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

impl Deserialize for ExistsRequest {
    fn deserialize(bytes: &mut BytesMut) -> Result<Self, ZkError> {
        let path = get_str(bytes)?;
        let watch = bytes.get_u8();
        let req = ExistsRequest {
            path,
            watch: watch != 0,
        };
        Ok(req)
    }
}

#[derive(Debug)]
pub struct GetAclRequest {
    pub path: String,
}

impl Record for GetAclRequest {
    fn serialize_into(&self, buffer: &mut BytesMut) -> Result<(), ZkError> {
        buffer.put_i32(self.path.len() as i32);
        buffer.extend_from_slice(self.path.as_bytes());
        Ok(())
    }

    fn size(&self) -> usize {
        4 + self.path.len()
    }
}

impl Deserialize for GetAclRequest {
    fn deserialize(bytes: &mut BytesMut) -> Result<Self, ZkError> {
        let path = get_str(bytes)?;
        let req = GetAclRequest {
            path,
        };
        Ok(req)
    }
}


#[derive(Debug)]
pub struct SetAclRequest {
    path: String,
    acl: Vec<Acl>,
    version: i32,
}

impl Record for SetAclRequest {
    fn serialize_into(&self, buffer: &mut BytesMut) -> Result<(), ZkError> {
        buffer.put_i32(self.path.len() as i32);
        buffer.extend_from_slice(self.path.as_bytes());
        buffer.put_i32(self.acl.len() as i32);
        for acl in &self.acl {
            buffer.put_i32(acl.perms);
            buffer.put_i32(acl.scheme.len() as i32);
            buffer.extend_from_slice(&acl.scheme);
            buffer.put_i32(acl.cred.len() as i32);
            buffer.extend_from_slice(&acl.cred);
        }
        buffer.put_i32(self.version);
        Ok(())
    }

    fn size(&self) -> usize {
        4 + self.path.len() + 4 + self.acl.iter().map(|a| 4 + 4 + a.scheme.len() + 4 + a.cred.len()).sum::<usize>() + 4
    }
}

impl Deserialize for SetAclRequest {
    fn deserialize(bytes: &mut BytesMut) -> Result<Self, ZkError> {
        let path = get_str(bytes)?;
        let acl_count = bytes.get_i32();
        let mut acl = Vec::with_capacity(acl_count as usize);
        for _ in 0..acl_count {
            let perms = bytes.get_i32();
            let scheme_len = bytes.get_i32();
            let scheme = bytes.split_to(scheme_len as usize).to_vec();
            let cred_len = bytes.get_i32();
            let cred = bytes.split_to(cred_len as usize).to_vec();
            acl.push(Acl {
                perms,
                scheme,
                cred,
            });
        }
        let version = bytes.get_i32();
        let req = SetAclRequest {
            path,
            acl,
            version,
        };
        Ok(req)
    }
}

#[derive(Debug)]
pub struct SetWatchesRequest {
    relative_zxid: i64,
    data_watches: Vec<String>,
    exist_watches: Vec<String>,
    child_watches: Vec<String>,
}

fn get_str_vec(bytes: &mut BytesMut) -> Result<Vec<String>, ZkError> {
    let count = bytes.get_i32();
    let mut vec = Vec::with_capacity(count as usize);
    for _ in 0..count {
        let len = bytes.get_i32();
        let s = bytes.split_to(len as usize);
        vec.push(String::from_utf8(s.to_vec()).map_err(|e| ZkError::InvalidString)?);
    }
    Ok(vec)
}

impl Record for SetWatchesRequest {
    fn serialize_into(&self, buffer: &mut BytesMut) -> Result<(), ZkError> {
        buffer.put_i64(self.relative_zxid);
        buffer.put_i32(self.data_watches.len() as i32);
        for path in &self.data_watches {
            buffer.put_i32(path.len() as i32);
            buffer.extend_from_slice(path.as_bytes());
        }
        buffer.put_i32(self.exist_watches.len() as i32);
        for path in &self.exist_watches {
            buffer.put_i32(path.len() as i32);
            buffer.extend_from_slice(path.as_bytes());
        }
        buffer.put_i32(self.child_watches.len() as i32);
        for path in &self.child_watches {
            buffer.put_i32(path.len() as i32);
            buffer.extend_from_slice(path.as_bytes());
        }
        Ok(())
    }

    fn size(&self) -> usize {
        8 + 4 + self.data_watches.iter().map(|p| 4 + p.len()).sum::<usize>() + 4
            + self.exist_watches.iter().map(|p| 4 + p.len()).sum::<usize>()
            + 4 + self.child_watches.iter().map(|p| 4 + p.len()).sum::<usize>()
    }
}

impl Deserialize for SetWatchesRequest {
    fn deserialize(bytes: &mut BytesMut) -> Result<Self, ZkError> {
        let relative_zxid = bytes.get_i64();
        let data_watches = get_str_vec(bytes)?;
        let exist_watches = get_str_vec(bytes)?;
        let child_watches = get_str_vec(bytes)?;
        let req = SetWatchesRequest {
            relative_zxid,
            data_watches,
            exist_watches,
            child_watches,
        };
        Ok(req)
    }
}

#[derive(Debug)]
pub enum Request {
    Connect(ConnectRequest),
    Create(CreateRequest),
    Delete(DeleteRequest),
    Exists(ExistsRequest),
    GetData(GetDataRequest),
    SetData(SetDataRequest),
    GetAcl(GetAclRequest),
    SetAcl(SetAclRequest),
    GetChildren(GetChildrenRequest),
    GetChildren2(GetChildrenRequest),
    Ping(PingRequest),
    GetChildren3(GetChildrenRequest),
    // Check(CheckRequest),
// Multi(MultiRequest),
    Close(PingRequest),
    SetWatches(SetWatchesRequest),
}

impl Record for Request {
    fn serialize_into(&self, buffer: &mut BytesMut) -> Result<(), ZkError> {
        match self {
            Request::Create(req) => { req.serialize_into(buffer) }
            Request::Delete(req) => { req.serialize_into(buffer) }
            Request::Exists(req) => { req.serialize_into(buffer) }
            Request::GetData(req) => { req.serialize_into(buffer) }
            Request::SetData(req) => { req.serialize_into(buffer) }
            Request::GetAcl(req) => { req.serialize_into(buffer) }
            Request::SetAcl(req) => { req.serialize_into(buffer) }
            Request::GetChildren(req) => { req.serialize_into(buffer) }
            Request::GetChildren2(req) => { req.serialize_into(buffer) }
            Request::Ping(req) => { req.serialize_into(buffer) }
            Request::GetChildren3(req) => { req.serialize_into(buffer) }
            Request::Close(req) => { req.serialize_into(buffer) }
            Request::SetWatches(req) => { req.serialize_into(buffer) }
            Request::Connect(req) => { req.serialize_into(buffer) }
        }
    }

    fn size(&self) -> usize {
        match self {
            Request::Create(req) => { req.size() }
            Request::Delete(req) => { req.size() }
            Request::Exists(req) => { req.size() }
            Request::GetData(req) => { req.size() }
            Request::SetData(req) => { req.size() }
            Request::GetAcl(req) => { req.size() }
            Request::SetAcl(req) => { req.size() }
            Request::GetChildren(req) => { req.size() }
            Request::GetChildren2(req) => { req.size() }
            Request::Ping(req) => { req.size() }
            Request::GetChildren3(req) => { req.size() }
            Request::Close(req) => { req.size() }
            Request::SetWatches(req) => { req.size() }
            Request::Connect(req) => { req.size() }
        }
    }
}

#[derive(Debug)]
pub struct RequestPacket {
    pub request_header: Option<RequestHeader>,
    pub request: Request,
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
pub struct ResponsePacket {
    pub response_header: Option<ReplyHeader>,
    pub response: ZkResponse,
}

impl Record for ResponsePacket {
    fn serialize_into(&self, buffer: &mut BytesMut) -> Result<(), ZkError> {
        if let Some(header) = &self.response_header {
            header.serialize_into(buffer)?;
        }
        self.response.serialize_into(buffer)?;
        Ok(())
    }

    fn size(&self) -> usize {
        self.response_header.as_ref().map_or(0, |h| h.size()) + self.response.size()
    }
}

#[derive(Debug)]
pub enum ZkResponse {
    Connect(ConnectResponse),
    GetData(GetDataResponse),
    GetChildren2 {
        children: Vec<String>,
        stat: Stat,
    },
    GetAcl {
        acl: Vec<Acl>,
        stat: Stat,
    },
    Ping,
    Stat(Stat),
    Empty,
    Strings(Vec<String>),
    String(String),
    Multi(Vec<Result<ZkResponse, ZkError>>),
}


impl Record for ZkResponse {
    fn serialize_into(&self, buffer: &mut bytes::BytesMut) -> Result<(), ZkError> {
        match self {
            ZkResponse::Connect(resp) => {
                resp.serialize_into(buffer)?;
            }
            ZkResponse::GetData(r) => {
                r.serialize_into(buffer)?;
            }
            ZkResponse::Ping => {}
            ZkResponse::GetAcl { acl, stat } => {
                buffer.put_i32(acl.len() as i32);
                for a in acl {
                    a.serialize_into(buffer)?;
                }
                stat.serialize_into(buffer)?;
            }
            ZkResponse::Stat(stat) => {
                stat.serialize_into(buffer)?;
            }
            ZkResponse::Empty => {}
            ZkResponse::Strings(strs) => {
                buffer.put_i32(strs.len() as i32);
                for s in strs {
                    buffer.put_i32(s.len() as i32);
                    buffer.put_slice(s.as_bytes());
                }
            }
            ZkResponse::String(str) => {
                buffer.put_i32(str.len() as i32);
                buffer.put_slice(str.as_bytes());
            }
            ZkResponse::Multi(_) => { todo!() }
            ZkResponse::GetChildren2 { children, stat } => {
                buffer.put_i32(children.len() as i32);
                for s in children {
                    buffer.put_i32(s.len() as i32);
                    buffer.put_slice(s.as_bytes());
                }
                stat.serialize_into(buffer)?;
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
                4 + r.data.len() + 6 * 8 + 4 * 5
            }
            ZkResponse::Ping => {
                0
            }
            ZkResponse::GetAcl { acl, stat } => {
                4 + acl.len() * 8 + stat.size()
            }
            ZkResponse::Stat(stat) => {
                stat.size()
            }
            ZkResponse::Empty => { 0 }
            ZkResponse::Strings(strs) => {
                4 + strs.iter().map(|s| 4 + s.len()).sum::<usize>()
            }
            ZkResponse::String(str) => {
                4 + str.len()
            }
            ZkResponse::Multi(_) => { todo!() }
            ZkResponse::GetChildren2 { children, stat } => {
                4 + children.iter().map(|s| 4 + s.len()).sum::<usize>() + stat.size()
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

impl Record for ConnectResponse {
    fn serialize_into(&self, buffer: &mut BytesMut) -> Result<(), ZkError> {
        buffer.put_i32(self.protocol_version);
        buffer.put_i32(self.timeout);
        buffer.put_i64(self.session_id);
        buffer.put_i32(self.passwd.len() as i32);
        buffer.extend_from_slice(&self.passwd);
        buffer.put_i8(self.read_only as i8);
        Ok(())
    }

    fn size(&self) -> usize {
        4 + 4 + 8 + 4 + self.passwd.len() + 1
    }
}

impl Deserialize for ConnectResponse {
    fn deserialize(bytes: &mut BytesMut) -> Result<Self, ZkError> {
        // let protocol_version = bytes.get_i32();
        let timeout = bytes.get_i32();
        let session_id = bytes.get_i64();
        let passwd_len = bytes.get_i32();
        let passwd = if passwd_len > 0 {
            let mut vec = vec![0; passwd_len as usize];
            bytes.copy_to_slice(&mut vec);
            vec
        } else {
            vec![]
        };
        let read_only = if bytes.len() > 0 {
            bytes.get_u8() == 1
        } else {
            false
        };
        Ok(Self {
            // protocol_version,
            protocol_version: 0,
            timeout,
            session_id,
            passwd,
            read_only,
        })
    }
}

#[derive(Debug)]
pub struct Stat {
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

impl Record for Stat {
    fn serialize_into(&self, buffer: &mut BytesMut) -> Result<(), ZkError> {
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

    fn size(&self) -> usize {
        6 * 8 + 4 * 5
    }
}


impl Deserialize for Stat {
    fn deserialize(bytes: &mut BytesMut) -> Result<Self, ZkError> {
        Ok(Self {
            czxid: bytes.get_i64(),
            mzxid: bytes.get_i64(),
            ctime: bytes.get_i64(),
            mtime: bytes.get_i64(),
            version: bytes.get_i32(),
            cversion: bytes.get_i32(),
            aversion: bytes.get_i32(),
            ephemeral_owner: bytes.get_i64(),
            data_length: bytes.get_i32(),
            num_children: bytes.get_i32(),
            pzxid: bytes.get_i64(),
        })
    }
}


#[derive(Debug)]
pub struct GetDataResponse {
    pub data: Vec<u8>,
    stat: Stat,
}

impl Record for GetDataResponse {
    fn serialize_into(&self, buffer: &mut bytes::BytesMut) -> Result<(), ZkError> {
        buffer.put_i32(self.data.len() as i32);
        buffer.extend_from_slice(&self.data);

        self.stat.serialize_into(buffer)?;

        Ok(())
    }

    fn size(&self) -> usize {
        4 + self.data.len() + self.stat.size()
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

pub struct ClientPacketCodec {
    inner: LengthDelimitedCodec,
    requests_by_xid: Arc<DashMap<i32, OpCodes>>,
}

impl ClientPacketCodec {
    pub fn new(map: Arc<DashMap<i32, OpCodes>>) -> Self {
        Self { inner: LENGTH_DELIMITED_CODEC.clone(), requests_by_xid: map }
    }
}

impl Encoder<RequestPacket> for ClientPacketCodec {
    type Error = ZkError;

    fn encode(&mut self, item: RequestPacket, dst: &mut bytes::BytesMut) -> Result<(), Self::Error> {
        if let Some(header) = &item.request_header {
            let xid = header.xid;
            let op = header.opcode;
            self.requests_by_xid.insert(xid, OpCodes::from_i32(op).ok_or(ZkError::EncodeError)?);
            println!("encode insert xid map :{:?} {:?}", xid, op);
        }

        let n = item.size();
        dst.reserve(n + 4);
        dst.put_i32(n as i32);
        item.serialize_into(dst)?;
        Ok(())
    }
}

impl Decoder for ClientPacketCodec {
    type Item = ResponsePacket;
    type Error = ZkError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        let src = self.inner.decode(src).map_err(|_| ZkError::DecodeError)?;

        println!("decode: {:?}", src);
        match src {
            None => { return Ok(None); }
            Some(mut src) => {
                let xid = src.get_i32();
                let xid_enum = XidCodes::from_i32(xid);

                if let Some(XidCodes::ConnectXid) = xid_enum {
                    let resp = ConnectResponse::deserialize(&mut src)?;
                    return Ok(Some(ResponsePacket {
                        response_header: None,
                        response: ZkResponse::Connect(resp),
                    }));
                }
                println!(">>>>>>>>..xid: {}", xid);
                let zxid = src.get_i64();
                let err = src.get_i32();
                let reply_header = ReplyHeader {
                    xid,
                    zxid,
                    err,
                };

                if xid_enum.is_some() {
                    match xid_enum.unwrap() {
                        XidCodes::ConnectXid => { unreachable!() }
                        XidCodes::WatchXid => {}
                        XidCodes::PingXid => {
                            return Ok(Some(ResponsePacket {
                                response_header: Some(reply_header),
                                response: ZkResponse::Ping,
                            }));
                        }
                        XidCodes::AuthXid => {}
                        XidCodes::SetWatchesXid => {}
                    }
                }
                println!("before: >>>>>>>>>>>>>>>>>>>>>>{:?},len:{}", xid, self.requests_by_xid.len());
                // for x in &self.requests_by_xid {
                //     println!(">>>>>>>.{:?}:{:?}", x.0, x.1);
                // }
                let opcode = self.requests_by_xid.get(&xid).ok_or(ZkError::DecodeError)?;
                let opcode = opcode.value().clone();
                println!("after   >>>>>>>>>>>>>>>>>>>>>>{:?}", opcode);

                match opcode {
                    OpCodes::Connect => {}

                    OpCodes::Exists | OpCodes::SetData | OpCodes::SetAcl => {
                        let stat = Stat::deserialize(&mut src)?;
                        return Ok(Some(ResponsePacket {
                            response_header: Some(reply_header),
                            response: ZkResponse::Stat(stat),
                        }));
                    }
                    OpCodes::GetData => {
                        let data = get_data(&mut src)?;
                        let stat = Stat::deserialize(&mut src)?;
                        println!(">>>>>>>>>>>>getdata>>>>>>>>>>{:?}", data);
                        println!(">>>>>>>>>>>>getdata>>>>>>>>>>{:?}", stat);
                        return Ok(Some(ResponsePacket {
                            response_header: Some(reply_header),
                            response: ZkResponse::GetData(GetDataResponse { data, stat }),
                        }));
                    }

                    OpCodes::Delete => {
                        return Ok(Some(ResponsePacket {
                            response_header: Some(reply_header),
                            response: ZkResponse::Empty,
                        }));
                    }
                    OpCodes::GetChildren => {
                        let children = get_str_vec(&mut src)?;
                        return Ok(Some(ResponsePacket {
                            response_header: Some(reply_header),
                            response: ZkResponse::Strings(children),
                        }));
                    }
                    OpCodes::Create => {
                        let path = get_str(&mut src)?;
                        return Ok(Some(ResponsePacket {
                            response_header: Some(reply_header),
                            response: ZkResponse::String(path),
                        }));
                    }
                    OpCodes::GetAcl => {
                        let acl = get_acl(&mut src)?;
                        let stat = Stat::deserialize(&mut src)?;
                        return Ok(Some(ResponsePacket {
                            response_header: Some(reply_header),
                            response: ZkResponse::GetAcl { acl, stat },
                        }));
                    }
                    OpCodes::Check => {
                        return Ok(Some(ResponsePacket {
                            response_header: Some(reply_header),
                            response: ZkResponse::Empty,
                        }));
                    }
                    OpCodes::Sync => {}
                    OpCodes::Ping => {}
                    OpCodes::GetChildren2 => {
                        let children = get_str_vec(&mut src)?;
                        let stat = Stat::deserialize(&mut src)?;
                        return Ok(Some(ResponsePacket {
                            response_header: Some(reply_header),
                            response: ZkResponse::GetChildren2 { children, stat },
                        }));
                    }
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
                }
            }
        }


        Ok(None)
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
                        // request: Box::new(req),
                        request: Request::Connect(req),
                    }));
                }
                XidCodes::WatchXid => {}
                XidCodes::PingXid => {
                    let opcode = src.get_i32();
                    return Ok(Some(RequestPacket {
                        request_header: Some(RequestHeader { xid, opcode }),
                        // request: Box::new(PingRequest {}),
                        request: Request::Ping(PingRequest {}),
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
                        // request: Box::new(req),
                        request: Request::GetData(req),
                    }));
            }
            OpCodes::Create | OpCodes::Create2 | OpCodes::CreateTtl | OpCodes::CreateContainer => {
                let req = CreateRequest::deserialize(src)?;
                return Ok(Some(
                    RequestPacket {
                        request_header: Some(RequestHeader { xid, opcode }),
                        // request: Box::new(req),
                        request: Request::Create(req),
                    }));
            }
            OpCodes::SetData => {
                let req = SetDataRequest::deserialize(src)?;
                return Ok(Some(
                    RequestPacket {
                        request_header: Some(RequestHeader { xid, opcode }),
                        request: Request::SetData(req),
                    }));
            }
            OpCodes::GetChildren | OpCodes::GetChildren2 => {
                let req = GetChildrenRequest::deserialize(src)?;
                return Ok(Some(
                    RequestPacket {
                        request_header: Some(RequestHeader { xid, opcode }),
                        request: Request::GetChildren(req),
                    }));
            }
            OpCodes::Delete => {
                let req = DeleteRequest::deserialize(src)?;
                return Ok(Some(
                    RequestPacket {
                        request_header: Some(RequestHeader { xid, opcode }),
                        request: Request::Delete(req),
                    }));
            }
            OpCodes::Exists => {
                let req = ExistsRequest::deserialize(src)?;
                return Ok(Some(
                    RequestPacket {
                        request_header: Some(RequestHeader { xid, opcode }),
                        request: Request::Exists(req),
                    }));
            }
            OpCodes::GetAcl => {
                let req = GetAclRequest::deserialize(src)?;
                return Ok(Some(
                    RequestPacket {
                        request_header: Some(RequestHeader { xid, opcode }),
                        request: Request::GetAcl(req),
                    }));
            }
            OpCodes::SetAcl => {
                let req = SetAclRequest::deserialize(src)?;
                return Ok(Some(
                    RequestPacket {
                        request_header: Some(RequestHeader { xid, opcode }),
                        request: Request::SetAcl(req),
                    }));
            }
            OpCodes::Sync => {}
            OpCodes::Check => {}
            OpCodes::Multi => {}
            OpCodes::Reconfig => {}
            OpCodes::CheckWatches => {}
            OpCodes::RemoveWatches => {}
            OpCodes::Close => {
                return Ok(Some(RequestPacket {
                    request_header: Some(RequestHeader { xid, opcode }),
                    request: Request::Close(PingRequest {}),
                }));
            }
            OpCodes::SetAuth => {}
            OpCodes::SetWatches => {
                let req = SetWatchesRequest::deserialize(src)?;
                return Ok(Some(
                    RequestPacket {
                        request_header: Some(RequestHeader { xid, opcode }),
                        request: Request::SetWatches(req),
                    }));
            }
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

impl Encoder<ResponsePacket> for ServerPacketCodec {
    type Error = ZkError;

    fn encode(&mut self, item: ResponsePacket, dst: &mut BytesMut) -> Result<(), Self::Error> {
        let mut buf = BytesMut::new();
        buf.reserve(item.size());
        item.serialize_into(&mut buf)?;
        self.inner.encode(buf.freeze(), dst).map_err(|_| ZkError::EncodeError)
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
