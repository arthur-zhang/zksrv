use std::sync::Arc;

use bytes::{BufMut, Bytes, BytesMut};
use dashmap::DashMap;
use futures::SinkExt;
use log::{debug, info};
use num_traits::ToPrimitive;
use tokio::net::{TcpListener, TcpStream};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio_stream::StreamExt;
use tokio_util::codec::{BytesCodec, FramedRead, FramedWrite};

use crate::codec::{ClientPacketCodec, Request, ResponsePacket, ServerPacketCodec, ZkResponse};
use crate::constants::OpCodes;
use crate::errors::ZkError;
use crate::proto::ReplyHeader;
use crate::record::Serialize;
use crate::zk_errcode::ZooErrors;

// c->p->b
pub struct UpStreamConnection {
    c2p_read_half: OwnedReadHalf,
    p2b_write_half: OwnedWriteHalf,

    tx: UnboundedSender<Bytes>,
}

impl UpStreamConnection {
    fn new(c2p_read_half: OwnedReadHalf, p2b_write_half: OwnedWriteHalf, tx: UnboundedSender<Bytes>) -> Self {
        return Self { c2p_read_half, p2b_write_half, tx };
    }

    async fn pipe(&mut self, map: Arc<DashMap<i32, OpCodes>>) -> std::io::Result<()> {
        let mut c2p_framed
            = FramedRead::new(&mut self.c2p_read_half, ServerPacketCodec::new());
        let mut p2b_framed
            = FramedWrite::new(&mut self.p2b_write_half, ClientPacketCodec::new(map.clone()));
        while let Some(Ok(r)) = c2p_framed.next().await {
            let xid = r.request_header.as_ref().and_then(|it| Some(it.xid)).clone();
            if let Request::GetChildren2(req) = &r.request {
                // if req.path == "/a".to_string() {
                //     let resp = ResponsePacket {
                //         response_header: Some(ReplyHeader {
                //             xid: xid.unwrap(),
                //             zxid: 0,
                //             err: ZooErrors::ZNOAUTH.to_i32().unwrap(),
                //         }),
                //         response: ZkResponse::Empty,
                //     };
                //     let mut bytes = BytesMut::with_capacity(resp.size()+4);
                //     bytes.put_i32(resp.size() as i32);
                //     resp.serialize_into(&mut bytes).unwrap();
                //     let _ = self.tx.send(bytes.freeze());
                //     continue;
                // }
            }
            let _ = p2b_framed.send(r).await;
        }
        Ok(())
    }
}


// c<-p
struct P2CDownStreamConnection {
    p2c_write_half: OwnedWriteHalf,
    rx: UnboundedReceiver<Bytes>,
}

impl P2CDownStreamConnection {
    fn new(p2c_write_half: OwnedWriteHalf, rx: UnboundedReceiver<Bytes>) -> Self {
        return Self { p2c_write_half, rx };
    }
    async fn pipe(&mut self, map: Arc<DashMap<i32, OpCodes>>) -> std::io::Result<()> {
        let mut writer = FramedWrite::new(&mut self.p2c_write_half, BytesCodec::new());

        while let Some(res) = self.rx.recv().await {
            let _ = writer.send(res).await;
            // let xid = res.response_header.as_ref().and_then(|it| Some(it.xid)).clone();
            // let _ = writer.send(res).await;
            // if let Some(xid) = xid {
            //     map.remove(&xid);
            // }
        }
        Ok(())
    }
}


pub struct B2PDownStreamConnection {
    b2p_read_half: OwnedReadHalf,
    tx: UnboundedSender<Bytes>,
}

impl B2PDownStreamConnection {
    fn new(b2p_read_half: OwnedReadHalf, tx: UnboundedSender<Bytes>) -> Self {
        return Self { b2p_read_half, tx };
    }
    async fn pipe(&mut self, map: Arc<DashMap<i32, OpCodes>>) -> std::io::Result<()> {
        let mut framed_reader = FramedRead::new(&mut self.b2p_read_half, BytesCodec::new());

        while let Some(Ok(res)) = framed_reader.next().await {
            let _ = self.tx.send(res.freeze());
        }
        Ok(())
    }
}

pub struct ZkServer {}

impl ZkServer {
    pub fn new() -> ZkServer {
        ZkServer {}
    }
    pub async fn handle_conn(c2p_stream: TcpStream) -> Result<(), ZkError> {
        let p2b_stream = TcpStream::connect("127.0.0.1:2181").await.unwrap();

        let (c2p_read_half, p2c_write_half) = c2p_stream.into_split();
        let (b2p_read_half, p2b_write_half) = p2b_stream.into_split();

        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let mut upstream_conn = UpStreamConnection::new(c2p_read_half, p2b_write_half, tx.clone());
        let mut p2c_downstream_conn = P2CDownStreamConnection::new(p2c_write_half, rx);
        let mut b2p_downstream_conn = B2PDownStreamConnection::new(b2p_read_half, tx.clone());

        let map = Arc::new(DashMap::new());
        tokio::select! {
            _ = upstream_conn.pipe(map.clone()) => {
                debug!("upstream_conn.pipe() done");
            }
            _ = p2c_downstream_conn.pipe(map.clone()) => {
                debug!("downstream_conn.pipe() done");
            }
            _ = b2p_downstream_conn.pipe(map.clone()) => {
                debug!("downstream_conn.pipe() done");
            }
        }
        Ok(())
    }

    pub async fn start(&self) -> Result<(), ZkError> {
        let listener = TcpListener::bind("[::]:2182").await.unwrap();
        println!("listen done");
        loop {
            let (c2p_socket, peer_addr) = listener.accept().await.unwrap();
            info!("peer_addr: {:?}", peer_addr);
            tokio::spawn(async move {
                if let Err(err) = Self::handle_conn(c2p_socket).await {
                    println!("handle_conn error: {:?}", err);
                }
            });
        }
        Ok(())
    }
}



