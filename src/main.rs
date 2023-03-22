mod codec;
mod errors;
mod proto;
mod server;
mod constants;
mod record;

use crate::codec::{Context, State,  ClientPacketCodec};
use crate::errors::ZkError;
use crate::proto::{ConnectRequest, ConnectResponse, GetDataRequest, ZkRequest, ZkResponse};
use failure::ResultExt;
use futures::*;
use std::marker::PhantomData;
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, TcpSocket, TcpStream};
use tokio_util::codec::{Decoder, Encoder, Framed};
use crate::server::ZkServer;


#[tokio::main]
async fn main() -> Result<(), ZkError> {
    println!("Hello, world!");
    // let mut zk = Zookeeper::connect("127.0.0.1:2181").await?;
    // zk.get("/hello").await?;

    let zk_server = ZkServer::new();
    zk_server.start().await;
    Ok(())
}
