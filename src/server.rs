use std::sync::mpsc::Receiver;
use bytes::{BufMut, BytesMut};
use futures::{SinkExt, StreamExt};
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, TcpStream};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::select;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio_util::codec::{BytesCodec, FramedRead, FramedWrite};
use tokio_util::either::Either;

use crate::codec::{ClientPacketCodec, ReplyHeader, Request, RequestPacket, ServerPacketCodec, ZkResponse};
use crate::errors::ZkError;
use crate::record::Record;

// c->p->b
pub struct UpStreamConnection {
    c2p_read_half: OwnedReadHalf,
    p2b_write_half: OwnedWriteHalf,

    tx: UnboundedSender<BytesMut>,
}

// c<-p<-b
pub struct DownStreamConnection {
    b2p_read_half: OwnedReadHalf,
    p2c_write_half: OwnedWriteHalf,
    rx: UnboundedReceiver<BytesMut>,
    tx: UnboundedSender<BytesMut>,
}

impl UpStreamConnection {
    fn new(c2p_read_half: OwnedReadHalf, p2b_write_half: OwnedWriteHalf, tx: UnboundedSender<BytesMut>) -> Self {
        return Self { c2p_read_half, p2b_write_half, tx };
    }

    async fn pipe(&mut self) -> std::io::Result<()> {
        let mut c2p_framed
            = FramedRead::new(&mut self.c2p_read_half, ServerPacketCodec::new());
        let mut p2b_framed
            = FramedWrite::new(&mut self.p2b_write_half, ClientPacketCodec::new());

        while let Some(Ok(r)) = FramedRead::next(&mut c2p_framed).await {
            println!("c->p: {:?}", r);
            if let Request::GetChildren(req) = &r.request {
                if req.path == "/".to_string() {
                    println!("get children of root");
                    let resp = ZkResponse::Ping(ReplyHeader { xid: r.request_header.as_ref().unwrap().xid, zxid: 0, err: -102 });
                    let mut bytes_mut = BytesMut::new();
                    bytes_mut.reserve(resp.size() + 4);
                    bytes_mut.put_i32(resp.size() as i32);
                    resp.serialize_into(&mut bytes_mut).unwrap();
                    let _ = self.tx.send(bytes_mut);
                    continue;
                }
            }
            let _ = p2b_framed.send(r).await;
        }
        Ok(())
    }
}

impl DownStreamConnection {
    fn new(b2p_read_half: OwnedReadHalf, p2c_write_half: OwnedWriteHalf, tx: UnboundedSender<BytesMut>, rx: UnboundedReceiver<BytesMut>) -> Self {
        return Self { b2p_read_half, p2c_write_half, tx, rx };
    }
    async fn pipe(&mut self) -> std::io::Result<u64> {
        let mut framed_reader = FramedRead::new(&mut self.b2p_read_half, BytesCodec::new());
        let mut tx = self.tx.clone();
        let mut writer = FramedWrite::new(&mut self.p2c_write_half, BytesCodec::new());
        loop {
            tokio::select! {
                Some(Ok(res)) = framed_reader.next()=>{
                    tx.send(res);
                }
                Some(res) = self.rx.recv() => {
                    println!("p->c: {:?}", res);
                    writer.send(res).await;
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

        tokio::select! {
            _ = upstream_conn.pipe() => {
                println!("upstream_conn.pipe() done");
            }
            _ = downstream_conn.pipe() => {
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



