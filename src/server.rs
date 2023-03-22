use futures::{SinkExt, StreamExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio_util::codec::{FramedRead, FramedWrite};

use crate::codec::{ClientPacketCodec, ServerPacketCodec};
use crate::errors::ZkError;

// c->p->b
pub struct UpStreamConnection {
    c2p_read_half: OwnedReadHalf,
    p2b_write_half: OwnedWriteHalf,
}

// c<-p<-b
pub struct DownStreamConnection {
    b2p_read_half: OwnedReadHalf,
    p2c_write_half: OwnedWriteHalf,
}

impl UpStreamConnection {
    fn new(c2p_read_half: OwnedReadHalf, p2b_write_half: OwnedWriteHalf) -> Self {
        return Self { c2p_read_half, p2b_write_half };
    }
    async fn pipe(&mut self) -> std::io::Result<()> {
        let mut c2p_framed
            = FramedRead::new(&mut self.c2p_read_half, ServerPacketCodec::new());
        let mut p2b_framed
            = FramedWrite::new(&mut self.p2b_write_half, ClientPacketCodec::new());

        while let Some(Ok(r)) = FramedRead::next(&mut c2p_framed).await {
            println!("c->p: {:?}", r);
            let _ = p2b_framed.send(r).await;
        }
        Ok(())
    }
}

impl DownStreamConnection {
    fn new(b2p_read_half: OwnedReadHalf, p2c_write_half: OwnedWriteHalf) -> Self {
        return Self { b2p_read_half, p2c_write_half };
    }
    async fn pipe(&mut self) -> std::io::Result<u64> {
        tokio::io::copy(&mut self.b2p_read_half, &mut self.p2c_write_half).await
    }
}

pub struct ZkServer {}

impl ZkServer {
    pub fn new() -> ZkServer {
        ZkServer {}
    }
    pub async fn handle_conn(c2p_stream: TcpStream, p2b_stream: TcpStream)
                             -> Result<(), ZkError> {
        let (c2p_read_half, p2c_write_half) = c2p_stream.into_split();
        let (b2p_read_half, p2b_write_half) = p2b_stream.into_split();

        let mut upstream_conn = UpStreamConnection::new(c2p_read_half, p2b_write_half);
        let mut downstream_conn = DownStreamConnection::new(b2p_read_half, p2c_write_half);

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
        let listener = TcpListener::bind("0.0.0.0:2182").await.unwrap();
        loop {
            let (c2p_socket, peer_addr) = listener.accept().await.unwrap();
            println!("peer_addr: {:?}", peer_addr);
            let p2b_stream = TcpStream::connect("127.0.0.1:2181").await.unwrap();
            Self::handle_conn(c2p_socket, p2b_stream).await?;
        }
        Ok(())
    }
}



