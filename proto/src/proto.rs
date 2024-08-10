use serde::{Serialize, Deserialize};

use crate::{error::MyError, utils::get_timestamp};

/*
    Protocol header:
        +----------+----------+-----------------------+
        |  Version | Msg Type | Fragment Offset (u16) |
        +----------+----------+-----------------------+
        |   Session ID (u16)  |   Sequence ID (u16)   |
        +---------------------+-----------------------+
        |             Timestamp (u32)                 |
        +---------------------------------------------+

* Version: protocol version
* Msg Type: enum ProtoType
* Fragment Offset: fragment delimiting hint.
* Session ID: session identifier (allocated by server in channel handshake)
* Sequence ID: sequence identifier (incremented by client/server separately in each packet)
* Timestamp: timestamp of the message (sent)
*/

#[derive(Debug, Serialize, Deserialize, Copy, Clone, Hash)]
#[repr(u8)]
pub enum ProtoType {
    Request,      // client: request to send (large) packets
    RequestACK,   // server: acknowledge the request
    Push,         // client: push (small or fragmented) packets
    PushACK,      // server: lastest fragmented id of received packet
    GetResponse,  // client: request to check if there is response packets
    NoResponse,   // server: no response packets
    Response,     // server: request to send (large) response packets <=> Request
    Pull,         // client: single request to pull fragmented response packets
    PullResponse, // server: small or fragmented response <=> Push
    Received,     // client: acknowledge the received response
    Abort,        // whenever received, forcely close the connection
}

/*
    This is basically a Request-Response protocol with some optimizations for large data.
    Each packet is sent and expected to be acknowledged.

    Client => Server:
    1. Client sends a Request to server / If packet is small enough, Push is directly issued. (goto 4)
    2. Server acknowledges the Request with RequestACK.
    3. Client concurrently sends multiple fragmented Push packets. Server processes and recombines them.
    4. Server responds with PushACK for each Push packet. (client should resend if not acked)

    Server => Client:
    5. Client sends GetResponse to server. This is usually triggered by timer event.
            If there is no packets from server, client should then receive NoResponse. (abort)
    6. Server sends Response to client / If packet is small enough, PullResponse is directly issued. (goto 8)
            In stateful connections, this packet can be directly received without previous GetResponse events.
    7. Client concurrently sends Pull packets to server for fragmented data.
    8. Server responds with PullResponse for each Pull packet. (client should resend if not responded)
    9. Client sends Received to notify the server. Same effect as GetResponse in server side (besides GC). (goto 5)
*/

#[derive(Debug, Serialize, Deserialize, Copy, Clone, Hash)]
pub struct ProtoHeader {
    pub version: u8,
    //msg_type: ProtoType, // serialized in body
    pub fragment_offset: u16,
    //pub session_id: u16, // implemented by comm_
    pub sequence_id: u16,
    pub timestamp: u32,
}

#[derive(Debug, Serialize, Deserialize, Copy, Clone, Hash)]
pub struct ProtoRequest {
    pub frame_count: u16,
}

#[derive(Debug, Serialize, Deserialize, Copy, Clone, Hash)]
pub struct ProtoRequestACK {
    pub need_pull: bool,
}

#[derive(Debug, Serialize, Deserialize, Clone, Hash)]
pub struct ProtoPush {
    pub data: Vec<u8>,
}

#[derive(Debug, Serialize, Deserialize, Copy, Clone, Hash)]
pub struct ProtoPushACK {
    pub need_pull: bool,
}

#[derive(Debug, Serialize, Deserialize, Copy, Clone, Hash)]
pub struct ProtoGetResponse {
}

#[derive(Debug, Serialize, Deserialize, Copy, Clone, Hash)]
pub struct ProtoNoResponse {
}

#[derive(Debug, Serialize, Deserialize, Copy, Clone, Hash)]
pub struct ProtoResponse {
    pub frame_count: u16,
}

#[derive(Debug, Serialize, Deserialize, Copy, Clone, Hash)]
pub struct ProtoPull {
}

#[derive(Debug, Serialize, Deserialize, Clone, Hash)]
pub struct ProtoPullResponse {
    pub data: Vec<u8>,
}

#[derive(Debug, Serialize, Deserialize, Clone, Hash)]
pub struct ProtoReceived {
}


#[derive(Debug, Serialize, Deserialize, Clone, Hash)]
pub struct ProtoAbort {
}

#[derive(Debug, Serialize, Deserialize, Clone, Hash)]
#[repr(u8)]
pub enum ProtoBody {
    Request(ProtoRequest),
    RequestACK(ProtoRequestACK),
    Push(ProtoPush),
    PushACK(ProtoPushACK),
    GetResponse(ProtoGetResponse),
    NoResponse(ProtoNoResponse),
    Response(ProtoResponse),
    Pull(ProtoPull),
    PullResponse(ProtoPullResponse),
    Received(ProtoReceived),
    Abort(ProtoAbort),
}

#[derive(Debug, Serialize, Deserialize, Clone, Hash)]
pub struct ProtoPacket {
    pub header: ProtoHeader,
    pub body: ProtoBody,
}

impl ProtoPacket {

    /// initialize a new packet with current timestamp
    pub fn from(header: ProtoHeader, body: ProtoBody) -> Self {
        ProtoPacket {
            header: ProtoHeader {
                timestamp: get_timestamp() as u32,
                ..header
            },
            body,
        }
    }

    pub fn from_body(body: ProtoBody) -> Self {
        ProtoPacket {
            header: ProtoHeader {
                version: 1,
                fragment_offset: 0,
                sequence_id: 0,
                timestamp: get_timestamp() as u32,
            },
            body,
        }
    }

    pub fn deserialize(data: &[u8]) -> Result<Self, MyError> {
        match bincode::deserialize(data) {
            Ok(packet) => Ok(packet),
            Err(e) => Err(MyError::InvalidPacket(format!("Failed to deserialize packet: {}", e))),
        }
    }

    pub fn serialize(&self) -> Result<Vec<u8>, MyError> {
        match bincode::serialize(self) {
            Ok(data) => Ok(data),
            Err(e) => Err(MyError::InvalidPacket(format!("Failed to serialize packet: {}", e))),
        }
    }

    pub fn get_hash(&self) -> u64 {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let mut hasher = DefaultHasher::new();
        self.hash(&mut hasher);
        hasher.finish()
    
    }

}

