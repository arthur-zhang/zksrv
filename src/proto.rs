use bytes::{Buf, BufMut, BytesMut};

use crate::errors::ZkError;
use crate::record::Record;

#[derive(Debug)]
pub struct ConnectRequest {
    pub protocol_version: i32,
    pub last_zxid_seen: i64,
    pub timeout: i32,
    pub session_id: i64,
    pub passwd: Vec<u8>,
    pub read_only: bool,
}

impl ConnectRequest {
    pub fn deserialize(mut bytes: &mut BytesMut) -> Self {
        let len = bytes.get_i32();
        let protocol_version = bytes.get_i32();
        let last_zxid_seen = bytes.get_i64();
        let timeout = bytes.get_i32();
        let session_id = bytes.get_i64();
        let passwd_len = bytes.get_i32();
        let mut passwd = vec![0; passwd_len as usize];
        let read_only = bytes.get_u8();
        ConnectRequest {
            protocol_version,
            last_zxid_seen,
            timeout,
            session_id,
            passwd,
            read_only: read_only != 0,
        }
    }
}

impl Record for ZkRequest {
    fn serialize_into(&self, buffer: &mut BytesMut) -> Result<(), ZkError> {
        match self {
            ZkRequest::GetData(req) => {
                buffer.put_i32(req.path.len() as i32);
                buffer.extend_from_slice(req.path.as_bytes());
                buffer.put_u8(req.watch as u8);
            }
            ZkRequest::Ping => {}
            ZkRequest::ConnectInit(r) => {
                buffer.put_i32(r.protocol_version);
                buffer.put_i64(r.last_zxid_seen);
                buffer.put_i32(r.timeout);
                buffer.put_i64(r.session_id);
                buffer.put_i32(r.passwd.len() as i32);
                buffer.extend_from_slice(&r.passwd);
                buffer.put_u8(r.read_only as u8);
            }
        }
        Ok(())
    }

    fn size(&self) -> usize {
        match self {
            ZkRequest::GetData(req) => {
                4 + req.path.len() + 1
            }
            ZkRequest::Ping => 0,
            ZkRequest::ConnectInit(r) => {
                4 + 8 + 4 + 8 + 4 + r.passwd.len() + 1
            }
        }
    }
}


#[derive(Debug)]
pub struct GetDataRequest {
    pub path: String,
    pub watch: bool,
}


#[derive(Debug)]
pub enum ZkRequest {
    ConnectInit(ConnectRequest),
    GetData(GetDataRequest),
    Ping,
}

#[derive(Debug)]
pub struct RequestPacket {
    pub request_header: Option<RequestHeader>,
    pub request: ZkRequest,
}

impl Record for RequestPacket {
    fn serialize_into(&self, buffer: &mut BytesMut) -> Result<(), ZkError> {
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
    fn serialize_into(&self, buffer: &mut BytesMut) -> Result<(), ZkError> {
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
    fn serialize_into(&self, buffer: &mut BytesMut) -> Result<(), ZkError> {
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
    fn serialize_into(&self, buffer: &mut BytesMut) -> Result<(), ZkError> {
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
    fn serialize_into(&self, buffer: &mut BytesMut) -> Result<(), ZkError> {
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
