use async_trait::async_trait;
use crate::error::MyError;
use super::Encrypt;

pub struct EncryptPlain;

impl EncryptPlain {
    pub fn new() -> Self {
        EncryptPlain {}
    }
}

#[async_trait]
impl Encrypt for EncryptPlain {
    async fn encrypt<'a>(&'a self, data: &'a [u8]) -> Result<&'a [u8], MyError> {
        Ok(data)
    }

    async fn decrypt<'a>(&'a self, data: &'a [u8]) -> Result<&'a [u8], MyError> {
        Ok(data)
    }

    async fn handshake0(&self) -> Result<Vec<u8>, MyError> {
        Ok(vec![0; 64])
    }

    async fn handshake1(&self, _data: &[u8]) -> Result<Vec<u8>, MyError> {
        Ok(vec![0; 64])
    }

    async fn handshake2(&self, _data: &[u8]) -> Result<Vec<u8>, MyError> {
        Ok(vec![0; 64])
    }

    async fn handshake3(&self, _data: &[u8]) -> Result<(), MyError> {
        Ok(())
    }
}
