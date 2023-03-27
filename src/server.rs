use std::collections::HashMap;
use std::sync::Arc;
use bytes::{BufMut, BytesMut};
use dashmap::DashMap;
use futures::{SinkExt, StreamExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio_util::codec::{BytesCodec, FramedRead, FramedWrite};

use crate::codec::{ClientPacketCodec, ReplyHeader, Request, ResponsePacket, ServerPacketCodec, ZkResponse};
use crate::constants::OpCodes;
use crate::errors::ZkError;
use crate::record::Record;

// c->p->b
pub struct UpStreamConnection {
    c2p_read_half: OwnedReadHalf,
    p2b_write_half: OwnedWriteHalf,

    tx: UnboundedSender<ResponsePacket>,
}

// c<-p<-b
pub struct DownStreamConnection {
    b2p_read_half: OwnedReadHalf,
    p2c_write_half: OwnedWriteHalf,
    rx: UnboundedReceiver<ResponsePacket>,
    tx: UnboundedSender<ResponsePacket>,
}

impl UpStreamConnection {
    fn new(c2p_read_half: OwnedReadHalf, p2b_write_half: OwnedWriteHalf, tx: UnboundedSender<ResponsePacket>) -> Self {
        return Self { c2p_read_half, p2b_write_half, tx };
    }

    async fn pipe(&mut self, map: Arc<DashMap<i32, OpCodes>>) -> std::io::Result<()> {
        let mut c2p_framed
            = FramedRead::new(&mut self.c2p_read_half, ServerPacketCodec::new());
        let mut p2b_framed
            = FramedWrite::new(&mut self.p2b_write_half, ClientPacketCodec::new(map.clone()));

        while let Some(Ok(r)) = FramedRead::next(&mut c2p_framed).await {
            println!("c->p: {:?}", r);
            let xid = r.request_header.as_ref().and_then(|it| Some(it.xid)).clone();
            if let Request::GetChildren(req) = &r.request {
                if req.path == "/".to_string() {
                    println!("get children of root");
                    let resp = ResponsePacket {
                        response_header: Some(ReplyHeader { xid: xid.unwrap(), zxid: 0, err: -102 }),
                        response: ZkResponse::Ping,
                    };
                    let _ = self.tx.send(resp);
                    continue;
                }
            }
            let _ = p2b_framed.send(r).await;
        }
        Ok(())
    }
}

impl DownStreamConnection {
    fn new(b2p_read_half: OwnedReadHalf, p2c_write_half: OwnedWriteHalf, tx: UnboundedSender<ResponsePacket>, rx: UnboundedReceiver<ResponsePacket>) -> Self {
        return Self { b2p_read_half, p2c_write_half, tx, rx };
    }
    async fn pipe(&mut self, map: Arc<DashMap<i32, OpCodes>>) -> std::io::Result<u64> {
        let mut framed_reader = FramedRead::new(&mut self.b2p_read_half, ClientPacketCodec::new(map.clone()));
        let tx = self.tx.clone();
        let mut writer = FramedWrite::new(&mut self.p2c_write_half, ServerPacketCodec::new());
        loop {
            tokio::select! {
                Some(Ok(res)) = framed_reader.next()=>{
                    tx.send(res);
                }
                Some(res) = self.rx.recv() => {
                    println!("p->c: {:?}", res);

                    let xid = res.response_header.as_ref().and_then(|it| Some(it.xid)).clone();
                    let _ = writer.send(res).await;
                    if let Some(xid) = xid {
                        map.remove(&xid);
                    }
                }
            }
        }
        Ok(0)
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

        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let mut upstream_conn = UpStreamConnection::new(c2p_read_half, p2b_write_half, tx.clone());
        let mut downstream_conn = DownStreamConnection::new(b2p_read_half, p2c_write_half, tx.clone(), rx);

        let map = Arc::new(DashMap::new());
        tokio::select! {
            _ = upstream_conn.pipe(map.clone()) => {
                println!("upstream_conn.pipe() done");
            }
            _ = downstream_conn.pipe(map.clone()) => {
                println!("downstream_conn.pipe() done");
            }
        }
        Ok(())
    }

    pub async fn start(&self) -> Result<(), ZkError> {
        let listener = TcpListener::bind("[::]:2182").await.unwrap();
        println!("listen done");
        loop {
            let (c2p_socket, peer_addr) = listener.accept().await.unwrap();
            println!("peer_addr: {:?}", peer_addr);
            tokio::spawn(async move {
                if let Err(err) = Self::handle_conn(c2p_socket).await {
                    println!("handle_conn error: {:?}", err);
                }
            });
        }
        Ok(())
    }
}



