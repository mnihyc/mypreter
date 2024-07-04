
pub const PROTO_VERSION: u8 = 1;

// time lag allowed between the packet construction and ultimate send
pub const CHANNEL_SENDWAIT_TIMEOUT: usize = 5;

// time lag allowed in expecting proto packet response
// or maximum time before retransmission of a packet
pub const CHANNEL_RECVWAIT_TIMEOUT: usize = 5;

// time interval between active GetResponse packets
pub const CHANNEL_GETRESPONSE_INTERVAL: usize = 3;

// maximum time of inactivity before the a completed sequence is cleared
pub const SEQUENCE_GC_INTERVAL: usize = 60;

// maximum time of inactivity before the a non-completed sequence is cleared
pub const SEQUENCE_GC_TIMEOUT: usize = 300;

// maximum time of inactivity before the session is closed
pub const SESSION_GC_TIMEOUT: usize = 600;


