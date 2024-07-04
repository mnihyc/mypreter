use serde::{Serialize, Deserialize};

use crate::error::MyError;

#[derive(Debug, Serialize, Deserialize, Copy, Clone)]
pub struct KeepAlive {
    pub timestamp: u64,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct BasicInfo {
    pub name: String,
    pub version: String,
    pub description: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct CmdExec {
    pub cmd: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct CmdExecRes {
    pub result: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[repr(u8)]
pub enum PacketPayload {
    KeepAlive(KeepAlive),
    BasicInfo(BasicInfo),
    CmdExec(CmdExec),
    CmdExecRes(CmdExecRes),
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Packet {
    pub payload: PacketPayload,
}

impl Packet {
    pub fn new(payload: PacketPayload) -> Self {
        Packet {
            payload,
        }
    }

    pub async fn deserialize(data: &[u8]) -> Result<Self, MyError> {
        let packet: Packet = bincode::deserialize(data).map_err(|e| MyError::InvalidPacket(format!("Deserialization error: {}", e.to_string())))?;
        Ok(packet)
    }

    pub async fn serialize(&self) -> Result<Vec<u8>, MyError> {
        bincode::serialize(&self).map_err(|e| MyError::InvalidPacket(format!("Serialization error: {}", e.to_string())))
    }
}


