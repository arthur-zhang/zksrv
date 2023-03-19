#[derive(Debug)]
pub enum ZkError {
    SocketError(String),
}

impl From<std::io::Error> for ZkError {
    fn from(e: std::io::Error) -> Self {
        ZkError::SocketError(e.to_string())
    }
}
