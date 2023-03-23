use std::fmt::Debug;
use crate::errors::ZkError;

pub trait Deserialize: Sized {
    fn deserialize(bytes: &mut bytes::BytesMut) -> Result<Self, ZkError>;
}

pub trait Record: Debug + Send + Sync {
    fn serialize_into(&self, buffer: &mut bytes::BytesMut) -> Result<(), ZkError>;
    fn size(&self) -> usize;
}

