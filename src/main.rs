mod codec;
mod errors;
mod proto;

use crate::codec::{Context, State, ClientAuthCodec, PacketCodec};
use crate::errors::ZkError;
use crate::proto::ZkRequest::Connect;
use crate::proto::{ConnectRequest, ConnectResponse, GetDataRequest, ZkRequest, ZkResponse};
use failure::ResultExt;
use futures::*;
use std::marker::PhantomData;
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, TcpSocket, TcpStream};
use tokio_util::codec::{Decoder, Encoder, Framed};

pub struct Zookeeper<T, C> {
    framed: Framed<T, C>,
    pub protocol_version: i32,
    pub timeout: i32,
    pub session_id: i64,
    pub passwd: Vec<u8>,
    pub read_only: bool,
}

impl Zookeeper<TcpStream, PacketCodec> {
    pub async fn connect(addr: &str) -> Result<Zookeeper<TcpStream, PacketCodec>, ZkError> {
        let stream = match TcpStream::connect(addr).await {
            Ok(stream) => stream,
            Err(err) => {
                return Err(ZkError::SocketError(format!(
                    "cannot connect to server {:?}",
                    err
                )));
            }
        };
        let codec = ClientAuthCodec::new();
        let mut framed = Framed::new(stream, codec);
        let connect_packet = ConnectRequest {
            protocol_version: 0,
            last_zxid_seen: 0,
            timeout: 1 * 1000,
            session_id: 0,
            passwd: vec![],
            read_only: false,
        };
        framed.send(ZkRequest::Connect(connect_packet)).await?;
        if let Some(Ok(resp)) = framed.next().await {
            if let ZkResponse::Connect(resp) = resp {
                println!("resp: {:?}", resp);

                let parts = framed.into_parts();
                let packet_codec = PacketCodec::new(parts.codec);

                let framed = Framed::new(parts.io, packet_codec);

                Ok(Zookeeper {
                    framed,
                    protocol_version: resp.protocol_version,
                    timeout: resp.timeout,
                    session_id: resp.session_id,
                    passwd: resp.passwd,
                    read_only: resp.read_only,
                })
            } else {
                unreachable!()
            }
        } else {
            Err(ZkError::SocketError("cannot connect to server".to_string()))
        }
    }
    async fn get(&mut self, path: &str) -> Result<(), ZkError> {
        println!("get");

        let req = GetDataRequest {
            path: path.to_string(),
            watch: false,
        };
        self.framed.send(ZkRequest::GetData(req)).await?;
        if let Some(Ok(resp)) = self.framed.next().await {
            println!("resp: {:?}", resp);
            Ok(())
        } else {
            println!(">>>>>>>>>>>...");
            Err(ZkError::SocketError("cannot connect to server".to_string()))
        }
        // let mut vec = Vec::new();
        // req.serialize_into(&mut vec).unwrap();
    }
}

#[tokio::main]
async fn main() -> Result<(), ZkError> {
    println!("Hello, world!");
    let mut zk = Zookeeper::connect("127.0.0.1:2181").await?;
    zk.get("/hello").await?;
    // zk.get("/hello");
    // let stream = TcpStream::connect("127.0.0.1:2181").await?;
    // let mut framed = Framed::new(stream, ZkCodec {});
    // framed.send(ConnectRequest {
    //     protocol_version: 0,
    //     last_zxid_seen: 0,
    //     timeout: 10 * 1000,
    //     session_id: 0,
    //     passwd: vec![],
    //     read_only: false,
    // }).await?;
    // loop {
    //     match framed.next().await {
    //         None => {
    //             println!("eof");
    //             break;
    //         }
    //         Some(Ok(res)) => {
    //             println!("res: {:?}", res);
    //         }
    //         Some(Err(err)) => {
    //             println!("err: {:?}", err);
    //             break;
    //         }
    //     }
    // }
    Ok(())
}
