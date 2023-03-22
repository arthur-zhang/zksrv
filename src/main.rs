use crate::errors::ZkError;
use crate::server::ZkServer;

mod codec;
mod errors;
mod server;
mod constants;
mod record;

#[tokio::main]
async fn main() -> Result<(), ZkError> {
    println!("Hello, world!");
    let zk_server = ZkServer::new();
    zk_server.start().await.unwrap();
    Ok(())
}
