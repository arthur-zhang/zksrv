use crate::errors::ZkError;
use crate::proto::ZkRequest::Connect;
use crate::proto::{ConnectRequest, ConnectResponse, GetDataResponse, ZkRequest, ZkResponse};
use bytes::{Buf, BufMut, BytesMut};
use tokio::io::AsyncWriteExt;
use tokio_util::codec::{Decoder, Encoder};

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

impl Encoder<ZkRequest> for ClientPacketCodec {
    type Error = ZkError;

    fn encode(&mut self, item: ZkRequest, dst: &mut BytesMut) -> Result<(), Self::Error> {
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

impl Decoder for ClientPacketCodec {
    type Item = ZkResponse;
    type Error = ZkError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
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
        println!("bytes len: {}", src.len());
        if src.len() < 4 {
            return Ok(None);
        }
        let len = src.get_i32();
        println!("len: {}", len);

        match self.next_state {
            State::ConnectStart => {
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
                self.next_state = State::ConnectDone;
                Ok(Some(ConnectResponse {
                    protocol_version,
                    timeout,
                    session_id,
                    passwd,
                    read_only: read_only == 1,
                }))
            }
            State::ConnectDone => {
                return Ok(None);
            }
            _ => {
                return Ok(None);
            }
        }
    }
}

impl tokio_util::codec::Encoder<ZkRequest> for ClientConnectCodec {
    type Error = std::io::Error;

    fn encode(&mut self, item: ZkRequest, dst: &mut bytes::BytesMut) -> Result<(), Self::Error> {
        let mut vec = Vec::new();
        let mut tmp = Vec::new();
        item.serialize_into(&mut tmp)?;
        // match self.next_state {
        //     State::Init => {}
        //     State::ConnectStart => {
        //         vec.extend_from_slice(&tmp);
        //     }
        //     State::ConnectDone => {}
        // }
        vec.extend_from_slice(&tmp);
        dst.put_i32(vec.len() as i32);
        dst.extend_from_slice(&vec);

        // if self.context.state == State::Connect {
        // } else {
        //     vec.put_i32(self.context.xid);
        //     vec.put_i32(4);
        //     vec.extend_from_slice(&tmp);
        // }


        Ok(())
    }
}
