use crate::errors::ZkError;
use crate::proto::ZkRequest::Connect;
use crate::proto::{ConnectRequest, ConnectResponse, GetDataResponse, ZkRequest, ZkResponse};
use bytes::{Buf, BufMut};
use tokio::io::AsyncWriteExt;

#[derive(Eq, PartialEq)]
pub enum State {
    None,
    Connect,
    Normal,
}

pub struct Context {
    pub state: State,
    pub xid: i32,
}

pub struct ZkCodec {
    pub context: Context,
}

impl tokio_util::codec::Decoder for ZkCodec {
    type Item = ZkResponse;
    type Error = ZkError;

    fn decode(&mut self, src: &mut bytes::BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        println!("bytes len: {}", src.len());
        if src.len() < 4 {
            return Ok(None);
        }
        let len = src.get_i32();
        println!("len: {}", len);
        match self.context.state {
            State::None => {
                return Ok(None);
            }
            State::Connect => {
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
                let mut passwd = Vec::with_capacity(passwd_len);
                src.copy_to_slice(&mut passwd);
                src.advance(passwd_len);
                let read_only = src.get_u8();
                self.context.state = State::Normal;
                Ok(Some(ZkResponse::Connect(ConnectResponse {
                    protocol_version,
                    timeout,
                    session_id,
                    passwd,
                    read_only: read_only == 1,
                })))
            }
            State::Normal => {
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
    }
}

impl tokio_util::codec::Encoder<ZkRequest> for ZkCodec {
    type Error = std::io::Error;

    fn encode(&mut self, item: ZkRequest, dst: &mut bytes::BytesMut) -> Result<(), Self::Error> {
        let mut vec = Vec::new();

        let mut tmp = Vec::new();
        item.serialize_into(&mut tmp)?;
        if self.context.state == State::Connect {
            vec.extend_from_slice(&tmp);
        } else {
            vec.put_i32(self.context.xid);
            vec.put_i32(4);
            vec.extend_from_slice(&tmp);
        }

        dst.put_i32(vec.len() as i32);
        dst.extend_from_slice(&vec);

        // dst.put_i32(buf.len() as i32);
        // dst.extend_from_slice(&buf);
        Ok(())
    }
}
