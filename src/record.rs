use crate::errors::ZkError;

pub trait Record {
    fn serialize_into(&self, buffer: &mut bytes::BytesMut) -> Result<(), ZkError>;
    fn size(&self) -> usize;
}

