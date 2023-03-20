use bytes::{Buf, BufMut, BytesMut};
use futures::{SinkExt, StreamExt};
use tokio::net::{TcpListener, TcpSocket, TcpStream};
use tokio_util::codec::{Decoder, Encoder, Framed};
use crate::codec::{ClientConnectCodec, ClientPacketCodec, State};
use crate::errors::ZkError;
use crate::proto::{ConnectRequest, ConnectResponse, ZkRequest, ZkResponse};

pub struct ZkServer {}

impl ZkServer {
    pub fn new() -> ZkServer {
        ZkServer {}
    }
    pub async fn start(&self) {
        let listener = TcpListener::bind("127.0.0.1:2182").await.unwrap();
        loop {
            let (socket, peer_addr) = listener.accept().await.unwrap();
            println!("peer_addr: {:?}", peer_addr);

            let server_stream = TcpStream::connect("127.0.0.1:2181").await.unwrap();
            let mut server_stream_framed = Framed::new(server_stream, ClientConnectCodec::new());

            let connect_codec = ServerConnectCodec {};
            let mut connect_framed = Framed::new(socket, connect_codec);
            // loop {
            if let Some(req) = connect_framed.next().await {
                let req = req.unwrap();
                server_stream_framed.send(req).await.unwrap();
                let server_resp = server_stream_framed.next().await.unwrap();
                match server_resp {
                    Err(err) => {
                        println!("err: {:?}", err);
                    }
                    Ok(resp) => {
                        connect_framed.send(resp).await.expect("should be ok");
                    }
                }

                // let parts = connect_framed.into_parts();
                // let packet_codec = ServerPacketCodec::new(parts.codec);
                // let mut packet_framed = Framed::new(parts.io, packet_codec);
                // while let req = packet_framed.next().await {
                let (mut c2s_r, mut c2s_w) = tokio::io::split(connect_framed.into_parts().io);
                let (mut s2b_r, mut s2b_w) = tokio::io::split(server_stream_framed.into_parts().io);
                let client_to_server = async {
                    tokio::io::copy(&mut c2s_r, &mut s2b_w).await
                };
                let server_to_client = async {
                    tokio::io::copy(&mut s2b_r, &mut c2s_w).await
                };
                let _ = tokio::try_join!(client_to_server, server_to_client).unwrap();
                // }
            }
        }
        // }
    }
}

struct ServerConnectCodec {}

impl tokio_util::codec::Decoder for ServerConnectCodec {
    type Item = ZkRequest;
    type Error = ZkError;

    fn decode(&mut self, src: &mut bytes::BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        println!("bytes len: {}", src.len());
        if src.len() < 4 {
            return Ok(None);
        }

        // self.next_state = State::ConnectDone;
        Ok(Some(ZkRequest::Connect(ConnectRequest::deserialize(src))))
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


struct ServerPacketCodec {}

impl ServerPacketCodec {
    pub fn new(connect_codec: ServerConnectCodec) -> Self {
        Self {}
    }
}

impl Encoder<ConnectResponse> for ServerPacketCodec {
    type Error = ZkError;

    fn encode(&mut self, item: ConnectResponse, dst: &mut BytesMut) -> Result<(), Self::Error> {
        // let mut vec = Vec::new();
        // {
        //     let mut tmp = Vec::new();
        //     item.serialize_into(&mut tmp)?;
        //     vec.put_i32(self.xid);
        //     vec.put_i32(4);
        //     vec.extend_from_slice(&tmp);
        // }
        //
        // dst.put_i32(vec.len() as i32);
        // dst.extend_from_slice(&vec);
        Ok(())
    }
}

impl Decoder for ServerPacketCodec {
    type Item = ZkRequest;
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
        // Ok(Some(ZkResponse::GetData(GetDataResponse {
        //     xid,
        //     zxid,
        //     err,
        //     data: buf,
        //     czxid: src.get_i64(),
        //     mzxid: src.get_i64(),
        //     ctime: src.get_i64(),
        //     mtime: src.get_i64(),
        //     version: src.get_i32(),
        //     cversion: src.get_i32(),
        //     aversion: src.get_i32(),
        //     ephemeral_owner: src.get_i64(),
        //     data_length: src.get_i32(),
        //     num_children: src.get_i32(),
        //     pzxid: src.get_i64(),
        // })))
        todo!()
    }
}
