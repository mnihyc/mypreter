use std::sync::Arc;

use crate::{encrypt::Encrypt, error::MyError, packet::Packet, proto::{ProtoAbort, ProtoBody, ProtoPacket, ProtoRequest}, session::Session};

// a channel is the intermediate layer connecting proto and packet / session and network
pub struct Channel {
    session: Arc<Session>,
    encrypt: Arc<dyn Encrypt>,
    chunk_size: usize,
}

impl Channel {
    pub fn new(session: Arc<Session>, encrypt: Arc<dyn Encrypt>, chunk_size: usize) -> Self {
        Channel { session, encrypt, chunk_size }
    }

    pub async fn send(&self, data: Packet) -> Result<(), MyError> {
        log::debug!("Sending packet: {:?}", data);
        let mut packet = ProtoPacket::from_body(ProtoBody::Abort(ProtoAbort {}));
        packet = self.session.with_packet(packet.clone());
        let sequence = self.session.get_sequence_or_create(self.session.is_client(), packet.header.sequence_id).await;
        let mut sequence = sequence.write().await;
        if sequence.len() != 0 {
            log::warn!("Packet sequence collision (sending too fast?): {:?}", packet.header.sequence_id);
            sequence.gc();
        }
        sequence.with_data(self.encrypt.encrypt(&data.serialize().await?).await?, self.chunk_size)?;
        let seqlen = sequence.len();
        drop(sequence); // note: release the lock

        if self.session.is_client() {
            packet = ProtoPacket::from(packet.header, ProtoBody::Request(ProtoRequest { frame_count: seqlen }));
            self.session.send_sequence(packet).await?;
        } else {
            self.session.send_passive(packet.header.sequence_id).await?;
        }
        Ok(())
    }

    pub async fn recv(&self) -> Result<Packet, MyError> {
        let data = self.session.recv_sequence().await?;
        let packet = Packet::deserialize(&self.encrypt.decrypt(&data).await?).await?;
        log::debug!("Received packet: {:?}", packet);
        Ok(packet)
    }
}

