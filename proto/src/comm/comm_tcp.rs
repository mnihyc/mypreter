use std::sync::Arc;

use crate::{encrypt::Encrypt, error::MyError, proto::{ProtoBody, ProtoPacket}, session::{Session, SessionManager}, utils::{AsyncFn, AsyncFuture}};

use super::{CommConnector, CommListener};
use async_trait::async_trait;
use tokio::{io::{AsyncReadExt, AsyncWriteExt}, net::{tcp::{OwnedReadHalf, OwnedWriteHalf}, TcpListener, TcpStream}, sync::Mutex};

async fn read_buf(stream: &mut OwnedReadHalf) -> Result<Vec<u8>, MyError> {
    let mut buf = [0; 4];
    if let Err(e) = stream.read_exact(&mut buf).await {
        return Err(MyError::Disconnected(format!("Failed to read packet: {}", e)));
    }
    let len = u32::from_le_bytes(buf[0..4].try_into().unwrap()) as u16 as usize;
    let mut data = vec![0; len];
    if let Err(e) = stream.read_exact(&mut data).await {
        return Err(MyError::Disconnected(format!("Failed to read packet: {}", e)));
    }
    Ok(data)
}

async fn write_buf(stream: &mut OwnedWriteHalf, data: &[u8]) -> Result<(), MyError> {
    let mut buf = [0; 4];
    buf[0..4].copy_from_slice(&(data.len() as u32).to_le_bytes());
    if let Err(e) = stream.write_all(&buf).await {
        return Err(MyError::Disconnected(format!("Failed to send packet: {}", e)));
    }
    if let Err(e) = stream.write_all(&data).await {
        return Err(MyError::Disconnected(format!("Failed to send packet: {}", e)));
    }
    if let Err(e) = stream.flush().await {
        return Err(MyError::Disconnected(format!("Failed to flush packet: {}", e)));
    }
    Ok(())
}

async fn handshake_client(stream: (Arc<Mutex<OwnedReadHalf>>, Arc<Mutex<OwnedWriteHalf>>), encrypt: Arc<dyn Encrypt>) -> Result<u16, MyError> {
    let mut stream = (stream.0.lock().await, stream.1.lock().await);
    if let Ok(data) = encrypt.handshake0().await {
        write_buf(&mut stream.1, &data).await?;
        let data = read_buf(&mut stream.0).await?;
        if let Ok(data) = encrypt.handshake2(&data).await {
            write_buf(&mut stream.1, &data).await?;
            let data = read_buf(&mut stream.0).await?;
            if let Ok(data) = encrypt.decrypt(&data).await {
                return Ok(u16::from_le_bytes(data[0..2].try_into().unwrap()));
            } else {
                return Err(MyError::CipherError("Failed to handshake session".to_string()));
            }
        } else {
            return Err(MyError::CipherError("Failed to handshake2".to_string()));
        }
    } else {
        return Err(MyError::CipherError("Failed to handshake0".to_string()));
    }
}

type GetSession = AsyncFn<(), Arc<Session>>;

async fn handshake_server(stream: (Arc<Mutex<OwnedReadHalf>>, Arc<Mutex<OwnedWriteHalf>>), encrypt: Arc<dyn Encrypt>, get_session: GetSession) -> Result<Arc<Session>, MyError> {
    let mut stream = (stream.0.lock().await, stream.1.lock().await);
    let data = read_buf(&mut stream.0).await?;
    if let Ok(data) = encrypt.handshake1(&data).await {
        write_buf(&mut stream.1, &data).await?;
        let data = read_buf(&mut stream.0).await?;
        if let Ok(_) = encrypt.handshake3(&data).await {
            let session = (get_session)(()).await;
            let data = session.get_id().to_le_bytes().to_vec();
            let data = encrypt.encrypt(&data).await?;
            write_buf(&mut stream.1, data).await?;
            Ok(session)
        } else {
            return Err(MyError::CipherError("Failed to handshake3".to_string()));
        }
    } else {
        return Err(MyError::CipherError("Failed to handshake1".to_string()));
    }
}

#[derive(Copy, Clone)]
pub struct TcpContextParam {
    pub workers: Option<usize>,
    pub keep_bind: Option<bool>,
}

impl TcpContextParam {
    pub fn new(workers: Option<usize>, keep_bind: Option<bool>) -> Self {
        TcpContextParam { workers, keep_bind }
    }
}

pub struct TcpContext {
    encrypt: Arc<dyn Encrypt>,
    is_client: bool,
    manager: Arc<SessionManager>,
    param: TcpContextParam,
}

impl TcpContext {
    pub fn new(encrypt: Arc<dyn Encrypt>, is_client: bool, manager: Arc<SessionManager>, param: TcpContextParam) -> Self {
        TcpContext { encrypt, is_client, manager, param }
    }

    async fn run(&self, stream: TcpStream) -> Result<Arc<Session>, MyError> {
        let (rstream, wstream) = stream.into_split();
        let rstream = Arc::new(Mutex::new(rstream));
        let rstream1 = rstream.clone();
        let wstream = Arc::new(Mutex::new(wstream));
        let wstream1 = wstream.clone();
        let sender = Arc::new(Box::new(move |args: (ProtoPacket, u16)| {
            let wstream = wstream1.clone();
            Box::pin(async move {
                let (packet, _) = args;
                log::debug!("Sending packet: {:?}", packet);
                let mut wstream = wstream.lock().await;
                let data = packet.serialize()?;
                write_buf(&mut wstream, &data).await?;
                if matches!(packet.body, ProtoBody::Abort(_)) {
                    // should close the connection
                    wstream.shutdown().await.ok();
                }
                Ok(())
            }) as AsyncFuture<Result<(), MyError>>
        }));

        let sender1 = sender.clone();
        let sender2 = sender.clone();
        let is_client1 = self.is_client;
        let manager1 = self.manager.clone();
        let param = self.param.clone();
        let get_session = Arc::new(Box::new(move |_| {
            let is_client = is_client1;
            let manager = manager1.clone();
            let sender = sender1.clone();
            Box::pin(async move {
                manager.create_session(manager.clone(), sender, is_client, 0, param.workers).await
            }) as AsyncFuture<Arc<Session>>
        }));

        let session;
        if self.is_client {
            let sid = handshake_client((rstream, wstream), self.encrypt.clone()).await?;
            session = self.manager.create_session(self.manager.clone(), sender, self.is_client, sid, self.param.workers).await;
            log::info!("Created session {}", session.get_id());
        } else {
            session = handshake_server((rstream, wstream), self.encrypt.clone(), get_session).await?;
            log::info!("Created session {}", session.get_id());
        }

        let manager1 = self.manager.clone();
        let rstream = rstream1.clone();
        let session_id = session.get_id();
        let remote_session_id = session.get_remote_id();
        tokio::spawn(async move {
            loop {
                let mut buf = [0; 4];
                let mut rstream = rstream.lock().await;
                if let Err(e) = rstream.read_exact(&mut buf).await {
                    log::error!("Failed to read packet: {}", e);
                    break;
                }
                let len = u32::from_le_bytes(buf[0..4].try_into().unwrap()) as u16 as usize;
                let mut data = vec![0; len];
                if let Err(e) = rstream.read_exact(&mut data).await {
                    log::error!("Failed to read packet: {}", e);
                    break;
                }
                if let Ok(packet) = ProtoPacket::deserialize(&data) {
                    match manager1.on_recv(packet, session_id).await {
                        Err(e) => {
                            log::error!("Failed to handle packet: {}", e);
                            break;
                        },
                        Ok(reply) => {
                            if let Some(reply) = reply {
                                if let Err(e) = sender2((reply, remote_session_id)).await {
                                    log::error!("Failed to send reply packet: {}", e);
                                    break;
                                }
                            }
                        
                        },
                    }
                } else {
                    log::error!("Failed to deserialize packet");
                    break;
                }
            }
            log::warn!("Closing connection");
            manager1.close_session(session_id).await;
        });
        Ok(session)
    }
}

pub struct CommTcpListener {
    hostr: String,
    context: TcpContext,
}

impl CommTcpListener {
    pub fn new(hostr: String, context: TcpContext) -> Self {
        CommTcpListener { hostr, context }
    }
}

#[async_trait]
impl CommListener for CommTcpListener {
    async fn bind(&self) -> Result<Option<Arc<Session>>, MyError> {
        log::info!("Binding to {}", self.hostr);
        match TcpListener::bind(self.hostr.clone()).await {
            Err(e) => return Err(MyError::NetworkError(format!("Failed to bind to {}: {}", self.hostr, e))),
            Ok(listener) => {
                return loop {
                    match listener.accept().await {
                        Err(e) => return Err(MyError::NetworkError(format!("Failed to accept connection: {}", e))),
                        Ok((stream, addr)) => {
                            log::info!("Accepted connection from {}", addr);

                            let r = self.context.run(stream).await;

                            match r {
                                Err(e) => {
                                    log::error!("Failed to handle connection: {}", e);
                                    continue;
                                },
                                Ok(session) => {
                                    if self.context.param.keep_bind.unwrap_or(false) {
                                        continue;
                                    } else {
                                        break Ok(Some(session));
                                    }
                                },
                            };
                        },
                    }
                }
            },
        }
    }
}

pub struct CommTcpConnector {
    hostr: String,
    context: TcpContext,
}

impl CommTcpConnector {
    pub fn new(hostr: String, context: TcpContext) -> Self {
        CommTcpConnector { hostr, context }
    }
}

#[async_trait]
impl CommConnector for CommTcpConnector {
    async fn connect(&self) -> Result<Arc<Session>, MyError> {
        match TcpStream::connect(self.hostr.clone()).await {
            Err(e) => Err(MyError::NetworkError(format!("Failed to connect to {}: {}", self.hostr, e))),
            Ok(stream) => {
                log::info!("Connected to {}", self.hostr);
                match self.context.run(stream).await {
                    Err(e) => Err(MyError::NetworkError(format!("Failed to handle connection: {}", e))),
                    Ok(session) => Ok(session),
                }
            },
        }
    }
}

