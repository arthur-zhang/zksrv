use std::io;
use std::io::Read;
use std::io::Write;
use byteorder::BigEndian;
use bytes::{Buf, BytesMut};

use crate::errors::ZkError;

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

impl ZkRequest {
    pub fn serialize_into(&self, buffer: &mut Vec<u8>) -> Result<(), io::Error> {
        match self {
            ZkRequest::Connect(req) => {
                use byteorder::{BigEndian, WriteBytesExt};
                buffer.write_i32::<BigEndian>(req.protocol_version)?;
                buffer.write_i64::<BigEndian>(req.last_zxid_seen)?;
                buffer.write_i32::<BigEndian>(req.timeout)?;
                buffer.write_i64::<BigEndian>(req.session_id)?;
                buffer.write_i32::<BigEndian>(req.passwd.len() as i32)?;
                buffer.write_all(&req.passwd)?;
                buffer.write_u8(req.read_only as u8)?;
            }
            ZkRequest::GetData(req) => {
                use byteorder::{BigEndian, WriteBytesExt};
                buffer.write_i32::<BigEndian>(req.path.len() as i32)?;
                buffer.write_all(req.path.as_bytes())?;
                buffer.write_u8(req.watch as u8)?;
            }
        }
        Ok(())
    }
}

pub struct GetDataRequest {
    pub path: String,
    pub watch: bool,
}


pub enum ZkRequest {
    Connect(ConnectRequest),
    GetData(GetDataRequest),
}

#[derive(Debug)]
pub enum ZkResponse {
    Connect(ConnectResponse),
    GetData(GetDataResponse),
}

#[derive(Debug)]
pub struct ConnectResponse {
    pub protocol_version: i32,
    pub timeout: i32,
    pub session_id: i64,
    pub passwd: Vec<u8>,
    pub read_only: bool,
}

impl ConnectResponse {
    pub fn serialize_into(&self, buffer: &mut Vec<u8>) -> Result<(), io::Error> {
        use byteorder::WriteBytesExt;
        buffer.write_i32::<BigEndian>(self.protocol_version)?;
        buffer.write_i32::<BigEndian>(self.timeout)?;
        buffer.write_i64::<BigEndian>(self.session_id)?;
        buffer.write_i32::<BigEndian>(self.passwd.len() as i32)?;
        buffer.write_all(&self.passwd)?;
        buffer.write_u8(self.read_only as u8)?;
        Ok(())
    }
}

#[derive(Debug)]
pub struct GetDataResponse {
    pub xid: i32,
    pub zxid: i64,
    pub err: i32,
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
