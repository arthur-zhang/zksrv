use std::time::Duration;
use byteorder::WriteBytesExt;
use bytes::{Buf, BufMut, BytesMut};
use futures::{SinkExt, StreamExt, TryFutureExt};
use num_traits::{FromPrimitive, ToPrimitive};
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, TcpSocket, TcpStream};
use tokio::time::sleep;
use tokio_util::codec::{Decoder, Encoder, Framed};
use crate::codec::{ClientConnectCodec, ClientPacketCodec, ensure_min_length, ServerConnectCodec, State};
use crate::constants::*;
use crate::errors::ZkError;
use crate::proto::{ConnectRequest, ConnectResponse, GetDataRequest, RequestHeader, RequestPacket, ZkRequest, ZkResponse};
use crate::record::Record;

pub struct ZkServer {}

impl ZkServer {
    pub fn new() -> ZkServer {
        ZkServer {}
    }
    pub async fn handle_handshake(mut s_stream: TcpStream, mut c_stream: TcpStream)
                                  -> Result<(Framed<TcpStream, ServerConnectCodec>, Framed<TcpStream, ClientConnectCodec>), ZkError> {
        let mut c2p: Framed<TcpStream, ServerConnectCodec> = Framed::new(s_stream, ServerConnectCodec {});
        let mut p2b: Framed<TcpStream, ClientConnectCodec> = Framed::new(c_stream, ClientConnectCodec::new());
        // 1. receive data from client
        if let Some(Ok(req)) = c2p.next().await {
            // 2. send to backend
            p2b.send(req).await?;

            // 3. wait fro backend resp
            if let Some(Ok(resp)) = p2b.next().await {
                // 4. send it to client
                c2p.send(resp).await?;
            }
        }
        Ok((c2p, p2b))
    }

    pub async fn handle_conn(mut c2p: Framed<TcpStream, ServerConnectCodec>, mut p2b: Framed<TcpStream, ClientConnectCodec>)
                             -> Result<(Framed<TcpStream, ServerPacketCodec>, Framed<TcpStream, ClientPacketCodec>), ZkError> {
        let mut c2p: Framed<TcpStream, ServerPacketCodec> = {
            let parts = c2p.into_parts();
            Framed::new(parts.io, ServerPacketCodec { xid: 0 })
        };
        let mut p2b: Framed<TcpStream, ClientPacketCodec> = {
            let parts = p2b.into_parts();
            Framed::new(parts.io, ClientPacketCodec { xid: parts.codec.xid })
        };
        println!("1. receive data from client");
        // 1. receive data from client
        while let Some(Ok(req)) = c2p.next().await {
            println!("receive: {:?}", req);
            println!("2. send to backend");
            // 2. send to backend
            p2b.send(req).await?;

            // 3. wait for backend resp
            println!("3. wait fro backend resp");
            if let Some(Ok(resp)) = p2b.next().await {
                println!("receive backend: {:?}", resp);
                println!("4. send it to client");
                // 4. send it to client
                c2p.send(resp).await?;
                println!(" send it to client done.....");
            }
        }
        Ok((c2p, p2b))
    }

    pub async fn start(&self) -> Result<(), ZkError> {
        let listener = TcpListener::bind("0.0.0.0:2182").await.unwrap();
        loop {
            let (socket, peer_addr) = listener.accept().await.unwrap();
            println!("peer_addr: {:?}", peer_addr);

            let c_stream = TcpStream::connect("127.0.0.1:2181").await.unwrap();

            let (c2p, p2b) = ZkServer::handle_handshake(socket, c_stream).await?;

            Self::handle_conn(c2p, p2b).await?;
            // sleep(Duration::from_secs(100)).await;
        }
        Ok(())
    }
}


struct ServerPacketCodec {
    xid: i32,
}

impl ServerPacketCodec {
    pub fn new(connect_codec: ServerConnectCodec) -> Self {
        Self {
            xid: 0
        }
    }
}

impl Encoder<ZkResponse> for ServerPacketCodec {
    type Error = ZkError;

    fn encode(&mut self, item: ZkResponse, dst: &mut BytesMut) -> Result<(), Self::Error> {
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

fn get_str(bytes: &mut BytesMut) -> Result<String, ZkError> {
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

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if src.len() < 4 {
            return Ok(None);
        }

        let packet_len = src.get_i32();
        ensure_min_length(packet_len, XID_LENGTH + INT_LENGTH)?; // xid + opcode
        let xid = src.get_i32();
        println!("xid: {}", xid);
        let opcode = src.get_i32();
        let opcode_enum = OpCodes::from_i32(opcode).unwrap();
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
                        request_header: RequestHeader { xid, opcode },
                        request: ZkRequest::GetData(req),
                    }));
            }
            OpCodes::Connect => {
                // return Ok(Some(ZkRequest::Connect(req)));
                todo!()
            }
            OpCodes::Ping => {
                return Ok(Some(
                    RequestPacket {
                        request_header: RequestHeader { xid, opcode },
                        request: ZkRequest::Ping,
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
        }
        unreachable!()
    }
}
