
#[derive(thiserror::Error, Debug)]
pub enum MyError {
    #[error("Network error: {0}")]
    NetworkError(String),
    #[error("Timeout reached")]
    Timeout,
    #[error("Disconnected from server: {0}")]
    Disconnected(String),
    #[error("Cipher error: {0}")]
    CipherError(String),
    #[error("Invalid packet received: {0}")]
    InvalidPacket(String),
    //#[error(transparent)]
    //Other(#[from] anyhow::Error),
}




