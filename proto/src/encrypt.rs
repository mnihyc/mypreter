use async_trait::async_trait;
use crate::error::MyError;

#[async_trait]
pub trait Encrypt: Send + Sync {
    async fn encrypt<'a>(&'a self, data: &'a [u8]) -> Result<&'a [u8], MyError>;
    async fn decrypt<'a>(&'a self, data: &'a [u8]) -> Result<&'a [u8], MyError>;

    // Handshake must be done within 1.5RTT; client must send first
    async fn handshake0(&self) -> Result<Vec<u8>, MyError>;
    async fn handshake1(&self, data: &[u8]) -> Result<Vec<u8>, MyError>;
    async fn handshake2(&self, data: &[u8]) -> Result<Vec<u8>, MyError>;
    async fn handshake3(&self, data: &[u8]) -> Result<(), MyError>;
}

pub mod enc_plain;
