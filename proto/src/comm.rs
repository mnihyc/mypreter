use std::sync::Arc;

use async_trait::async_trait;
use crate::{error::MyError, session::Session};

#[async_trait]
pub trait CommListener: Send + Sync {
    async fn bind(&self) -> Result<Option<Arc<Session>>, MyError>;
}

#[async_trait]
pub trait CommConnector: Send + Sync {
    async fn connect(&self) -> Result<Arc<Session>, MyError>;
}

pub mod comm_tcp;
