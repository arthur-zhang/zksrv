use crate::errors::ZkError;
use crate::server::ZkServer;

mod codec;
mod errors;
mod server;
mod constants;
mod record;
mod length_codec;
mod zk_errcode;
mod proto;

#[tokio::main]
async fn main() -> Result<(), ZkError> {
    println!("Hello, world!");
    env_logger::init();

    let zk_server = ZkServer::new();
    zk_server.start().await.unwrap();
    Ok(())
}
