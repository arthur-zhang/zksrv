use crate::errors::ZkError;
use crate::proto::ZkRequest::Connect;
use crate::proto::{ConnectRequest, ConnectResponse, GetDataResponse, ZkRequest, ZkResponse};
use bytes::{Buf, BufMut};
use tokio::io::AsyncWriteExt;
use crate::constants::*;

#[derive(Eq, PartialEq)]
pub enum State {
    Init,
    ConnectStart,
    ConnectDone,
}

pub struct Context {
    pub state: State,
    pub xid: i32,
}

pub struct ClientPacketCodec {
    pub xid: i32,
}

impl ClientPacketCodec {
    pub fn new(auth_codec: ClientConnectCodec) -> Self {
        Self {
            xid: auth_codec.xid,
        }
    }
}

pub fn ensure_min_length(len: i32, min: i32) -> Result<(), ZkError> {
    if len < min {
        return Err(ZkError::InvalidPacketLength(len));
    }
    Ok(())
}

impl tokio_util::codec::Encoder<ZkRequest> for ClientPacketCodec {
    type Error = ZkError;

    fn encode(&mut self, item: ZkRequest, dst: &mut bytes::BytesMut) -> Result<(), Self::Error> {
        let mut vec = Vec::new();
        {
            let mut tmp = Vec::new();
            item.serialize_into(&mut tmp)?;
            vec.put_i32(self.xid);
            vec.put_i32(4);
            vec.extend_from_slice(&tmp);
        }

        dst.put_i32(vec.len() as i32);
        dst.extend_from_slice(&vec);
        Ok(())
    }
}

impl tokio_util::codec::Decoder for ClientPacketCodec {
    type Item = ZkResponse;
    type Error = ZkError;

    fn decode(&mut self, src: &mut bytes::BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if src.len() < 4 {
            return Ok(None);
        }
        let packet_len = src.get_i32();
        println!("packet len: {}", packet_len);

        let xid = src.get_i32();
        println!("xid: {}", xid);
        let zxid = src.get_i64();
        let err = src.get_i32();
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
            xid,
            zxid,
            err,
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

pub struct ClientConnectCodec {
    pub next_state: State,
    pub xid: i32,
}


impl ClientConnectCodec {
    pub fn new() -> Self {
        Self {
            next_state: State::ConnectStart,
            xid: 0,
        }
    }
}

impl tokio_util::codec::Decoder for ClientConnectCodec {
    type Item = ConnectResponse;
    type Error = ZkError;

    fn decode(&mut self, src: &mut bytes::BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        let bytes_len = src.get_i32();

        println!("bytes len: {}", src.len());
        if src.len() < 4 {
            return Ok(None);
        }
        let protocol_version = src.get_i32();
        println!("protocol_version: {}", protocol_version);
        let timeout = src.get_i32();
        println!("timeout: {}", timeout);
        let session_id = src.get_i64();
        println!("session_id: {}", session_id);
        let passwd_len = src.get_i32() as usize;
        println!("passwd_len: {}", passwd_len);
        if src.len() < passwd_len + 1 {
            return Ok(None);
        }
        let mut passwd = vec![0; passwd_len];
        src.copy_to_slice(&mut passwd);
        // src.advance(passwd_len);
        let read_only = src.get_u8();
        Ok(Some(ConnectResponse {
            protocol_version,
            timeout,
            session_id,
            passwd,
            read_only: read_only == 1,
        }))
    }
}

impl tokio_util::codec::Encoder<ConnectRequest> for ClientConnectCodec {
    type Error = std::io::Error;

    fn encode(&mut self, item: ConnectRequest, dst: &mut bytes::BytesMut) -> Result<(), Self::Error> {
        let mut vec = Vec::new();
        let mut tmp = Vec::new();
        item.serialize_into(&mut tmp)?;

        vec.extend_from_slice(&tmp);
        dst.put_i32(vec.len() as i32);
        dst.extend_from_slice(&vec);
        Ok(())
    }
}

pub fn maybe_read_bool(bytes: &mut bytes::BytesMut) -> bool {
    return if bytes.remaining() >= 1 {
        bytes.get_u8() == 1
    } else {
        false
    };
}

pub struct ServerConnectCodec {}

impl tokio_util::codec::Decoder for ServerConnectCodec {
    type Item = ConnectRequest;
    type Error = ZkError;

    fn decode(&mut self, bytes: &mut bytes::BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        let connect_req = {
            let len = bytes.get_i32();
            ensure_min_length(len as i32, XID_LENGTH + ZXID_LENGTH + TIMEOUT_LENGTH + SESSION_LENGTH + INT_LENGTH)?;
            let protocol_version = bytes.get_i32();
            let last_zxid_seen = bytes.get_i64();
            let timeout = bytes.get_i32();
            let session_id = bytes.get_i64();
            let passwd_len = bytes.get_i32();
            let mut passwd = vec![0; passwd_len as usize];
            bytes.copy_to_slice(&mut passwd);
            let read_only = maybe_read_bool(bytes);
            ConnectRequest {
                protocol_version,
                last_zxid_seen,
                timeout,
                session_id,
                passwd,
                read_only,
            }
        };
        Ok(Some(connect_req))
    }
}

impl tokio_util::codec::Encoder<ConnectResponse> for ServerConnectCodec {
    type Error = std::io::Error;

    fn encode(&mut self, item: ConnectResponse, dst: &mut bytes::BytesMut) -> Result<(), Self::Error> {
        let mut tmp = Vec::new();
        item.serialize_into(&mut tmp)?;
        dst.put_i32(tmp.len() as i32);
        dst.extend_from_slice(&tmp);
        Ok(())
    }
}

