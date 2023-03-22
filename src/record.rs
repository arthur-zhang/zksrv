use std::fmt::Debug;
use crate::errors::ZkError;

pub trait Record: Debug {
    fn serialize_into(&self, buffer: &mut bytes::BytesMut) -> Result<(), ZkError>;
    fn size(&self) -> usize;
    // fn deserialize(bytes: &mut bytes::BytesMut) -> Self;
}

