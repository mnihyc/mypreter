// Intermediate layer: encapsulate packet into underlying proto.
// session management, retransmission, fragmentation, etc.

use std::{sync::Arc, time::Duration};

use scc::{HashMap, HashSet};
use async_channel;
use tokio::{sync::RwLock, time};

use crate::{consts::{CHANNEL_GETRESPONSE_INTERVAL, CHANNEL_RECVWAIT_TIMEOUT, CHANNEL_SENDWAIT_TIMEOUT, SEQUENCE_GC_INTERVAL, SEQUENCE_GC_TIMEOUT, SESSION_GC_TIMEOUT}, error::MyError, proto::{ProtoAbort, ProtoBody, ProtoGetResponse, ProtoHeader, ProtoNoResponse, ProtoPacket, ProtoPull, ProtoPullResponse, ProtoPush, ProtoPushACK, ProtoReceived, ProtoRequestACK, ProtoResponse}, utils::{get_timestamp, AsyncFn, AsyncFuture, Counter}};

pub struct SessionManager {
    session_id_cnt: Counter,   // session_id incrementer

    // on_recv() is called when new proto packet is received

    // session_id -> session
    sessions: Arc<HashMap<u16, Arc<Session>>>,
}

type Sender = AsyncFn<(ProtoPacket, u16), Result<(), MyError>>;

pub struct Session {
    session_id: u16,      // my session_id
    remote_session_id: u16, // remote session_id (required by clients)
    act_client: bool,     // true if this session is a client actor
    sequence_id_cnt: Counter, // current (sent) sequence_id incrementer
    last_active: Counter, // timestamp of last activity (used in GC)

    init: Counter,      // is run() initialized?
    manager: Arc<SessionManager>, // reference to session manager
    
    send_tx: async_channel::Sender<ProtoPacket>,
    send_rx: async_channel::Receiver<ProtoPacket>,
    
    // limits the speed of sending packets
    worker_threads: Arc<Counter>, // how many worker threads are kept running
    retr_map: HashSet<u64>, // retransmission control; avoid flooding the sending queue

    sender: Sender,       // sender for this session

    // sequence_id -> sequence (separate to avoid sid collision)
    sequences0: HashMap<u16, Arc<RwLock<Sequence>>>, // client -> server; push
    sequences1: HashMap<u16, Arc<RwLock<Sequence>>>, // client <- server; pull

    // contains a list of sequence_id that fills the server's pending send / client's pending recv
    server_send0: HashSet<u16>, // first priority sending queue / pending RequestACK queue
    server_send1: HashSet<u16>, // removed when first Pull request comes / handling recv queue
    recv_rx: async_channel::Receiver<Vec<u8>>, // packet fully received; upper recv() waitlist
    recv_tx: async_channel::Sender<Vec<u8>>,   //
    wait_rx: async_channel::Receiver<()>,  // any packets to receive?
    wait_tx: async_channel::Sender<()>, // notify upper channel waitlist
}

pub struct Sequence {
    total: u16,    // total number of fragments (total=1 means no fragmentation and fully received)
    received: u16, // cached number of received fragments (or sent by server)
    fragments: Vec<Fragment>,

    last_active: u32, // timestamp of last activity (used in GC and retransmission)
}

#[derive(Clone)]
pub struct Fragment {
    // length zero <=> fragment not received / acked
    data: Vec<u8>,
}

impl SessionManager {
    pub fn new() -> Self {
        Self {
            session_id_cnt: Counter::new(0),
            sessions: Arc::new(HashMap::new()),
        }
    }

    pub async fn close(&mut self) {
        for session in 0..(self.session_id_cnt.get() as u16) {
            self.close_session(session as u16).await;
        }
        self.sessions.clear();
    }

    /// Arc<SessionManager> is a self-reference obtained from owner (required by Rust)
    pub async fn create_session(&self, manager: Arc<SessionManager>, sender: Sender, act_client: bool, remote_session_id: u16, workers: Option<usize>) -> Arc<Session> {
        let session_id = self.session_id_cnt.increment() as u16;
        let session = Arc::new(Session::new(session_id, remote_session_id, act_client, sender, manager));
        if self.sessions.insert_async(session_id, session.clone()).await.is_err() {
            log::warn!("Session ID collision detected: {}", session_id);
            log::warn!("?Under DoS attack?");
            self.sessions.update_async(&session_id, |_, s| *s = session.clone()).await;
        }
        session.run(workers.unwrap_or(1)).await;
        session
    }

    pub async fn close_session(&self, session_id: u16) {
        if let Some(session) = self.sessions.remove_async(&session_id).await {
            tokio::spawn(async move {
                session.1.gc();
                // notify the other end
                let _ = (session.1.sender)((session.1.with_packet(
                    ProtoPacket::from_body(ProtoBody::Abort(ProtoAbort {}))
                ), session.1.remote_session_id)).await.ok();
            });
        }
    }

    pub async fn get_session(&self, session_id: u16) -> Option<Arc<Session>> {
        match self.sessions.get_async(&session_id).await {
            None => None,
            Some(session) => Some(session.clone()),
        }
    }

    pub async fn get_session_id(&self) -> Vec<u16> {
        let mut sids = Vec::new();
        self.sessions.scan_async(|k, _| {
            sids.push(*k);
        }).await;
        sids.sort();
        sids.dedup();
        sids
    }

    /// on_recv() is called when new proto packet is received.
    /// 
    /// for clients, it triggers wait_channel for state transitions.
    /// for servers, the returned packet should be responded by the caller.
    pub async fn on_recv(&self, packet: ProtoPacket, session_id: u16) -> Result<Option<ProtoPacket>, MyError> {
        log::debug!("Received packet: {:?}", packet);
        match self.get_session(session_id).await {
            None => {
                log::warn!("Dropping packet due to unknown session: {:?}", packet);
            }
            Some(session) => {
                session.activate();
                return session.on_recv(packet).await;
            }
        }
        Ok(None)
    }

}

// is the original packet ACKed (or regarded received)?
type RetrCond = AsyncFn<ProtoPacket, Result<bool, MyError>>;

impl Session {
    pub fn new(session_id: u16, remote_session_id: u16, act_client: bool, sender: Sender, manager: Arc<SessionManager>) -> Self {
        let (tx, rx) = async_channel::unbounded();
        let (tx1, rx1) = async_channel::unbounded();
        let (tx2, rx2) = async_channel::bounded(1);
        Self {
            session_id,
            remote_session_id,
            act_client,
            sequence_id_cnt: Counter::new(0),
            last_active: Counter::new(get_timestamp()),
            init: Counter::new(0),
            manager,
            send_tx: tx1,
            send_rx: rx1,
            retr_map: HashSet::new(),
            sender,
            worker_threads: Arc::new(Counter::new(0)),
            sequences0: HashMap::new(),
            sequences1: HashMap::new(),
            server_send0: HashSet::new(),
            server_send1: HashSet::new(),
            recv_rx: rx,
            recv_tx: tx,
            wait_rx: rx2,
            wait_tx: tx2,
        }
    }

    pub fn activate(&self) {
        self.last_active.set(get_timestamp());
    }

    pub fn gc(&self) {
        self.sequence_id_cnt.set(0);
        self.last_active.set(0);
        self.sequences0.clear();
        self.sequences1.clear();
        self.server_send0.clear();
        self.server_send1.clear();
        self.send_rx.close();
        self.send_tx.close();
        self.recv_rx.close();
        self.recv_tx.close();
        self.wait_rx.close();
        self.wait_tx.close();
    }

    pub fn is_client(&self) -> bool {
        self.act_client
    }

    pub fn get_id(&self) -> u16 {
        self.session_id
    }

    pub fn get_remote_id(&self) -> u16 {
        self.remote_session_id
    }

    pub fn with_packet(&self, packet: ProtoPacket) -> ProtoPacket {
        self.with_packet_sid(packet, self.sequence_id_cnt.increment() as u16)
    }

    pub fn with_packet_sid(&self, packet: ProtoPacket, sequence_id: u16) -> ProtoPacket {
        let mut packet = packet;
        packet.header.sequence_id = sequence_id;
        packet.header.timestamp = get_timestamp() as u32;
        packet
    }

    pub async fn get_sequence(&self, aop: bool, sequence_id: u16) -> Result<Arc<RwLock<Sequence>>, MyError> {
        let sequences = if aop { &self.sequences0 } else { &self.sequences1 };
        match sequences.get_async(&sequence_id).await {
            None => Err(MyError::InvalidPacket("Unknown sequence ID".to_string())),
            Some(sequence) => Ok({
                sequence.write().await.activate(); // this may slows down the process
                sequence.clone()
            }),
        }
    }

    pub async fn get_sequence_or_create(&self, aop: bool, sequence_id: u16) -> Arc<RwLock<Sequence>> {
        let sequence = {
            // (get and insert) operation need to be atomic; TODO here
            let sequences = if aop { &self.sequences0 } else { &self.sequences1 };
            loop {
                let sequence = Arc::new(RwLock::new(Sequence::new(0)));
                match sequences.insert_async(sequence_id, sequence.clone()).await {
                    Ok(_) => break sequence,
                    Err(_) => match sequences.get_async(&sequence_id).await {
                        None => continue,
                        Some(sequence) => break sequence.clone()
                    },
                }
            }
        };
        sequence.write().await.activate(); // this may slows down the process
        sequence
    }

    pub async fn remove_sequence(&self, aop: bool, sequence_id: u16) -> Option<(u16, Arc<RwLock<Sequence>>)> {
        let sequences = if aop { &self.sequences0 } else { &self.sequences1 };
        sequences.remove_async(&sequence_id).await
    }

    /// call this function to actively send a proto packet without any handling
    pub async fn send(&self, packet: ProtoPacket) -> Result<(), MyError> {
        if self.send_tx.send(packet).await.is_err() {
            // session need to be re-established
            return Err(MyError::Disconnected("Send channel closed".to_string())); // ambiguous error
        }
        Ok(())
    }

    /// clients call this function to actively send a packet with retransmission handling
    /// 
    /// the packet with type Request and its sequence must be already constructed
    pub async fn send_sequence(&self, packet: ProtoPacket) -> Result<(), MyError> {
        assert!(matches!(packet.body, ProtoBody::Request(_)));
        let session = self.session().await?;
        session.activate();
        // add this to pending RequestACK queue
        session.server_send0.insert_async(packet.header.sequence_id).await.ok();
        self.retransmission_send(vec![packet; 1], Arc::new(Box::new(move |packet: ProtoPacket| {
            let session = session.clone();
            Box::pin(async move {
                Ok(!session.server_send0.contains_async(&packet.header.sequence_id).await)
            }) as AsyncFuture<Result<bool, MyError>>
        }))).await?;
        Ok(())
    }

    /// servers call this function to passively send a packet by putting it in the send queue
    /// 
    /// the packet with type Request and its sequence must be already constructed
    pub async fn send_passive(&self, sequence_id: u16) -> Result<(), MyError> {
        assert!(self.get_sequence(false, sequence_id).await.is_ok());
        if self.send_tx.is_closed() { // same error as send_sequence
            return Err(MyError::Disconnected("Send channel closed".to_string()));
        }
        self.activate();
        self.server_send0.insert_async(sequence_id).await.ok();
        // in tcp/udp protocols, session.sender is not a dummy function
        // we can directly issue Response/PullResponse here to reduce interaction latency
        {
            // V same as on_recv GetResponse V
            let sequence = self.get_sequence(false, sequence_id).await?;
            let sequence = sequence.read().await;
            if sequence.total == 0 {
                // sequence may already be removed by another thread
                return Ok(());
            }
            if sequence.total == 1 {
                // packet is small enough; allow direct PullResponse
                let data = sequence.fragments[0].data.clone();
                let reply = self.with_packet_sid(ProtoPacket::from_body(
                    ProtoBody::PullResponse(ProtoPullResponse { data })
                ), sequence_id);
                (self.sender)((reply, self.remote_session_id)).await.ok(); // do not go through self.send()
                return Ok(());
            } else {
                // fragmentation required
                let frame_count = sequence.total;
                let reply = self.with_packet_sid(ProtoPacket::from_body(
                    ProtoBody::Response(ProtoResponse { frame_count })
                ), sequence_id);
                (self.sender)((reply, self.remote_session_id)).await.ok(); // do not go through self.send()
                return Ok(());
            }
        }
    }

    /// call this function to actively receive a sequence data
    pub async fn recv_sequence(&self) -> Result<Vec<u8>, MyError> {
        match self.recv_rx.recv().await {
            Ok(data) => Ok(data),
            Err(_) => Err(MyError::Disconnected("Recv channel closed".to_string())),
        }
    }

    /// get the session owned by manager (required by Rust)
    pub async fn session(&self) -> Result<Arc<Session>, MyError> {
        self.manager.get_session(self.session_id).await.ok_or(MyError::InvalidPacket("Session terminated during handling".to_string()))
    }

    /// (Re-)Run the session with specified number of sending workers
    /// 
    /// takes to time to re-adjust; may not always be accurate. do not change frequently.
    pub async fn run(&self, workers: usize) {
        // dynamically adjust tasks (not using signals for simplicity)
        while self.worker_threads.get() < workers {
            let tid = self.worker_threads.increment();
            tokio::spawn({
                // get a Arc reference (required by Rust)
                let session = self.session().await.unwrap();
                let manager = self.manager.clone();

                async move { // sender tasks
                    while tid < session.worker_threads.get() {
                        if let Ok(mut packet) = session.send_rx.recv().await {
                            if packet.header.timestamp + (CHANNEL_SENDWAIT_TIMEOUT as u32) < get_timestamp() as u32 {
                                log::warn!("Packet sending waitlist too long: {:?}", packet);
                                log::warn!("?System overload?");
                            }
                            packet.header.timestamp = get_timestamp() as u32;

                            // mark as actually sent
                            session.retr_map.remove_async(&packet.get_hash()).await;

                            match (session.sender)((packet.clone(), session.remote_session_id)).await {
                                Ok(_) => {}
                                Err(e) => {
                                    // Explicit error; simply retransmit (may cause overload)
                                    log::warn!("Failed to send packet: {:?}", e);
                                    match e {
                                        MyError::Disconnected(_) => {
                                            // session closed; need to be re-established
                                            manager.close_session(session.session_id).await;
                                        }
                                        _ => {
                                            // retransmit
                                            session.send(packet).await.ok(); // ignore error
                                        }
                                    }
                                }
                            }

                        } else {
                            break;
                        }
                    }
                }
            });
        }
        self.worker_threads.set(workers);

        if self.init.get() > 0 {
            return;
        }
        self.init.set(1);

        // a keepalive recving task for clients
        if self.is_client() {
            tokio::spawn({
                let session = self.session().await.unwrap();
                async move {
                    loop {
                        if session.wait_rx.is_closed() {
                            break;
                        }
                        let mut interval = time::interval(Duration::from_secs(CHANNEL_GETRESPONSE_INTERVAL as u64));
                        interval.tick().await; // note: first tick is immediate
                        tokio::select! {
                            _ = interval.tick() => { }
                            _ = session.wait_rx.recv() => { }
                        };
                        if session.send(session.with_packet(
                            ProtoPacket::from_body(ProtoBody::GetResponse(ProtoGetResponse {}))
                        )).await.is_err() {
                            break;
                        }
                    }
                }
            });
        }

        // run a task for sequence GC
        tokio::spawn({
            let session = self.session().await.unwrap();
            async move {
                loop {
                    if ((session.last_active.get() + SESSION_GC_TIMEOUT) as u32) < get_timestamp() as u32 {
                        session.manager.close_session(session.session_id).await;
                        break;
                    }
                    tokio::time::sleep(Duration::from_secs(SEQUENCE_GC_INTERVAL as u64)).await;
                    let clean = move |aop, session: Arc<Session>| {
                        async move {
                            let sequences = if aop { &session.sequences0 } else { &session.sequences1 };
                            let mut sids = Vec::new();
                            sequences.scan_async(|sid, _| {
                                sids.push(*sid);
                            }).await;
                            for sid in sids {
                                let sequence = match sequences.get_async(&sid).await {
                                    None => continue,
                                    Some(sequence) => sequence.clone(),
                                };
                                let sequence = sequence.read().await;
                                if (sequence.last_active + (SEQUENCE_GC_TIMEOUT as u32) < get_timestamp() as u32) || sequence.total == 0
                                 || (sequence.total == 1 && sequence.received >= 1 && sequence.last_active + (SEQUENCE_GC_INTERVAL as u32) < get_timestamp() as u32) {
                                    sequences.remove_async(&sid).await;
                                }
                            }
                        }
                    };
                    clean(true, session.clone()).await;
                    clean(false, session.clone()).await;
                }
            }
        });
    }

    fn expect_client(&self) -> Result<(), MyError> {
        if !self.act_client {
            return Err(MyError::InvalidPacket("Server received client packet".to_string()));
        }
        Ok(())
    }

    fn expect_server(&self) -> Result<(), MyError> {
        if self.act_client {
            return Err(MyError::InvalidPacket("Client received server packet".to_string()));
        }
        Ok(())
    }

    async fn new_sequence(&self, aop: bool, sequence_id: u16, frame_count: u16) -> Result<(), MyError> {
        if frame_count == 0 {
            return Err(MyError::InvalidPacket("Zero frame count".to_string()));
        }
        let sequence = self.get_sequence_or_create(aop, sequence_id).await;
        let mut sequence = sequence.write().await;
        if sequence.total != 0 {
            return Err(MyError::InvalidPacket("Packet sequence collision".to_string()));
        }
        sequence.total = frame_count;
        sequence.fragments = (0..sequence.total).map(|_| Fragment::new()).collect();
        sequence.activate();
        Ok(())
    }

    async fn process_data_packet(&self, aop: bool, packet: ProtoPacket, data: Vec<u8>) -> Result<bool, MyError> {
        if data.len() == 0 {
            return Err(MyError::InvalidPacket("Empty data packet".to_string()));
        }
        let mut ext_data = None;
        let finished = loop {
            let sequence = self.get_sequence_or_create(aop, packet.header.sequence_id).await;
            let mut sequence = sequence.write().await;
            if sequence.total == 0 {
                if packet.header.fragment_offset == 0 {
                    // packet is small enough; allow direct Push/PullResponse
                    sequence.total = 1;
                    sequence.fragments = vec![Fragment::new(); 1];
                } else {
                    // this may occur when packets arrive out of order;
                    // however, we keep packets until it expires, so it shouldn't happen
                    sequence.gc();
                    self.remove_sequence(aop, packet.header.sequence_id).await;
                    return Err(MyError::InvalidPacket("Packet data before allocation".to_string()));
                }
            }
            let fid = packet.header.fragment_offset as usize;
            if sequence.fragments.len() <= fid {
                if sequence.total == 1 && sequence.received >= sequence.total {
                    // packet is already fully received and reconstructed; silently ignore
                    break false
                }
                sequence.gc();
                return Err(MyError::InvalidPacket("Fragment ID out of range".to_string()));
            }
            if sequence.fragments[fid].data.len() != 0 {
                log::warn!("Fragment already received: {:?}", fid);
                // may be duplicated packets; skip it
                break false;
            } else {
                sequence.received += 1;
            }
            sequence.fragments[fid].data = data;
            if sequence.received == sequence.total {
                let mut data: Vec<u8> = Vec::new();
                for fragment in sequence.fragments.iter() {
                    data.extend(fragment.data.iter());
                }
                sequence.fragments = vec![Fragment::new_with(data.clone()); 1];
                sequence.total = 1;
                
                // packet fully received; notify waiting recv()s
                ext_data = Some(data);
                break true;
            }
            break false;
        };

        if let Some(data) = ext_data {
            // GC is done in initially spawned task
            self.recv_tx.send(data).await.ok();
        }

        return Ok(finished);
    }

    /// manager delegates on_recv to certain session
    pub async fn on_recv(&self, packet: ProtoPacket) -> Result<Option<ProtoPacket>, MyError> {
        let session = self.session().await?;
        match packet.body.clone() {
            /* server process; retransmission is done by client */
            ProtoBody::Request(data) => {
                self.expect_server()?;
                self.new_sequence(true, packet.header.sequence_id, data.frame_count).await?;

                let reply = ProtoPacket::from(packet.header, ProtoBody::RequestACK(ProtoRequestACK {
                    need_pull: !session.server_send0.is_empty() || !session.server_send1.is_empty()
                }));
                return Ok(Some(reply));
            },

            ProtoBody::Push(data) => {
                self.expect_server()?;
                self.process_data_packet(true, packet.clone(), data.data).await?;

                let reply = ProtoPacket::from(packet.header, ProtoBody::PushACK(ProtoPushACK {
                    need_pull: !session.server_send0.is_empty() || !session.server_send1.is_empty()
                }));
                return Ok(Some(reply));
            },

            ProtoBody::GetResponse(_) | ProtoBody::Received(_) => {
                self.expect_server()?;
                if matches!(packet.body, ProtoBody::Received(_)) {
                    // Received implies the packet is fully received by client
                    // safe to remove from sequences?
                    session.remove_sequence(false, packet.header.sequence_id).await;
                    session.server_send1.remove_async(&packet.header.sequence_id).await;
                    session.server_send0.remove_async(&packet.header.sequence_id).await;
                }

                // GetResponse does not require a valid sequence id
                // acts as a hint for server to send response
                let mut sequence_id = None;
                while sequence_id.is_none() {
                    session.server_send0.any_async(|x| {
                        sequence_id = Some(*x);
                        true
                    }).await;
                    // these operations should be atomic; but alright to have duplicated ones
                    if sequence_id.is_none() {
                        session.server_send1.any_async(|x| {
                            sequence_id = Some(*x);
                            true
                        }).await;
                    } else {
                        // move from top proiroty to second; we have probability to believe the client have known this sequence
                        // this gives more chance for client to pull different sequence
                        session.server_send0.remove_async(&sequence_id.unwrap()).await;
                        session.server_send1.insert_async(sequence_id.unwrap()).await.ok();
                    }
                    if sequence_id.is_none() {
                        // we are sure there is no packet to send (is it safe not to send this packet?)
                        let reply = ProtoPacket::from(packet.header, ProtoBody::NoResponse(ProtoNoResponse {}));
                        return Ok(Some(reply));
                    } else {
                        // 
                        let sequence_id = sequence_id.unwrap();
                        let sequence = session.get_sequence(false, sequence_id).await?;
                        let sequence = sequence.read().await;
                        if sequence.total == 0 {
                            // sequence may already be removed by another thread
                            continue;
                        }
                        if sequence.total == 1 {
                            // packet is small enough; allow direct PullResponse
                            let data = sequence.fragments[0].data.clone();
                            let reply = session.with_packet_sid(ProtoPacket::from_body(
                                ProtoBody::PullResponse(ProtoPullResponse { data })
                            ), sequence_id);
                            return Ok(Some(reply));
                        } else {
                            // fragmentation required
                            let frame_count = sequence.total;
                            let reply = session.with_packet_sid(ProtoPacket::from_body(
                                ProtoBody::Response(ProtoResponse { frame_count })
                            ), sequence_id);
                            return Ok(Some(reply));
                        }
                    }
                }
            },

            ProtoBody::Pull(_) => {
                self.expect_server()?;
                // client should have known the existence of the sequence; remove from server_send1
                session.server_send1.remove_async(&packet.header.sequence_id).await;

                let sequence = session.get_sequence(false, packet.header.sequence_id).await?;
                let sequence = sequence.read().await;
                if sequence.total == 0 {
                    // this may occur when packets arrive out of order; silently ignore
                    return Ok(None);
                }
                if packet.header.fragment_offset >= sequence.total {
                    return Err(MyError::InvalidPacket("Pull fragment out of range".to_string()));
                }
                let data = sequence.fragments[packet.header.fragment_offset as usize].data.clone();
                let reply = ProtoPacket::from(packet.header, ProtoBody::PullResponse(ProtoPullResponse { data }));
                return Ok(Some(reply));
            },

            /* client process; manage states internally */
            ProtoBody::RequestACK(data) => {
                self.expect_client()?;
                if data.need_pull {
                    session.wait_tx.try_send(()).ok();
                }
                if session.server_send0.remove_async(&packet.header.sequence_id).await.is_none() {
                    // duplicated RequestACK packet; another task is handling this
                    return Ok(None)
                }
                let sequence = session.get_sequence(true, packet.header.sequence_id).await?;
                let sequence = sequence.read().await;
                let mut packets = Vec::new();
                for i in 0..sequence.total {
                    let packet = ProtoPacket::from(ProtoHeader {
                        fragment_offset: i,
                        ..packet.header.clone()
                    }, ProtoBody::Push(ProtoPush { data: sequence.fragments[i as usize].data.clone() }));
                    packets.push(packet);
                }
                let session1 = session.clone();
                self.retransmission_send(packets, Arc::new(Box::new(move |packet: ProtoPacket| {
                    let session = session1.clone();
                    Box::pin(async move {
                        let sequence = match session.get_sequence(true, packet.header.sequence_id).await {
                            Ok(sequence) => sequence,
                            Err(_) => return Ok(true),
                        };
                        // either it's removed; or not fully acked
                        let sequence = sequence.read().await;
                        if sequence.fragments.len() <= packet.header.fragment_offset as usize {
                            // fragment out of range; should exit
                            return Err(MyError::InvalidPacket("Fragment ID out of range".to_string()));
                        }
                        if sequence.fragments[packet.header.fragment_offset as usize].data.len() == 0 {
                            // fragment already acked
                            return Ok(true);
                        }
                        Ok(false)
                    }) as AsyncFuture<Result<bool, MyError>>
                }))).await?;
            },

            ProtoBody::PushACK(data) => {
                self.expect_client()?;
                if data.need_pull {
                    session.wait_tx.try_send(()).ok();
                }
                let sequence = session.get_sequence(true, packet.header.sequence_id).await?;
                let mut sequence = sequence.write().await;
                let fid = packet.header.fragment_offset as usize;
                if sequence.fragments.len() <= fid {
                    return Err(MyError::InvalidPacket("Fragment ID out of range".to_string()));
                }
                if sequence.fragments[fid].data.len() > 0 {
                    sequence.received += 1;
                    sequence.fragments[fid].data = Vec::new(); // clear the fragment (acked)
                }
                if sequence.received == sequence.total {
                    // packet is fully sent and acked; remove directly
                    sequence.gc();
                    session.remove_sequence(true, packet.header.sequence_id).await;
                }
            },

            ProtoBody::NoResponse(_) => {
                self.expect_client()?;
                // no response; do nothing
            },

            ProtoBody::Response(data) => {
                self.expect_client()?;
                if session.server_send1.insert_async(packet.header.sequence_id).await.is_err() {
                    // duplicated Response packet; another task is handling this
                    return Ok(None)
                }
                if self.new_sequence(false, packet.header.sequence_id, data.frame_count).await.is_err() {
                    // either invalid data or this is a duplicated packet; ignore
                    return Ok(None)
                }
                let mut packets = Vec::new();
                for i in 0..data.frame_count {
                    let packet = ProtoPacket::from(ProtoHeader {
                        fragment_offset: i,
                        ..packet.header.clone()
                    }, ProtoBody::Pull(ProtoPull {}));
                    packets.push(packet);
                }
                let session1 = session.clone();
                self.retransmission_send(packets, Arc::new(Box::new(move |packet: ProtoPacket| {
                    let session = session1.clone();
                    Box::pin(async move {
                        let sequence = match session.get_sequence(false, packet.header.sequence_id).await {
                            Ok(sequence) => sequence,
                            Err(_) => return Ok(true),
                        };
                        let sequence = sequence.read().await;
                        if sequence.total == 0 {
                            // sequence may already be removed by another thread
                            return Ok(true);
                        }
                        if sequence.total == 1 && sequence.received >= sequence.total {
                            // packet is fully received and reconstructed
                            return Ok(true);
                        }
                        if sequence.fragments.len() <= packet.header.fragment_offset as usize {
                            // fragment out of range; retransmit
                            return Err(MyError::InvalidPacket("Fragment ID out of range".to_string()));
                        }
                        if sequence.fragments[packet.header.fragment_offset as usize].data.len() == 0 {
                            // fragment not acked; retransmit
                            return Ok(false);
                        }
                        Ok(true)
                    }) as AsyncFuture<Result<bool, MyError>>
                }))).await?;
            },

            ProtoBody::PullResponse(data) => {
                self.expect_client()?;
                let finished = self.process_data_packet(false, packet.clone(), data.data).await?;
                if finished {
                    session.server_send1.remove_async(&packet.header.sequence_id).await;
                    session.send(session.with_packet_sid(
                        ProtoPacket::from_body(ProtoBody::Received(ProtoReceived {})),
                        packet.header.sequence_id
                    )).await.ok();
                }
            },

            /* abort */
            ProtoBody::Abort(_) => {
                self.manager.close_session(self.session_id).await;
            },
            
        }
        Ok(None)
    }

    pub async fn retransmission_send(&self, packets: Vec<ProtoPacket>, cond: RetrCond) -> Result<(), MyError> {
        // it's very unlikely to encounter hash collision; but if it does, it's okay to ignore
        for packet in packets.clone() {
            self.retr_map.insert_async(packet.get_hash()).await.ok();
            self.send(packet.clone()).await?;
        }
        tokio::spawn({
            let session = self.session().await?;
            let mut packets = packets.clone();
            async move {
                'outer: loop {
                    if packets.len() == 0 {
                        break;
                    }
                    tokio::time::sleep(Duration::from_secs(CHANNEL_RECVWAIT_TIMEOUT as u64)).await;
                    let mut packets1 = Vec::new();
                    for packet in packets.iter() {
                        match (cond)(packet.clone()).await {
                            Ok(true) => {},
                            Ok(false) => {
                                // considered lost; retransmit
                                if !session.retr_map.contains_async(&packet.get_hash()).await {
                                    // sent and not acked; maybe lost
                                    let packet = ProtoPacket::from(packet.header, packet.body.clone());
                                    packets1.push(packet.clone());
                                    session.retr_map.insert_async(packet.get_hash()).await.ok();
                                    if session.send(packet.clone()).await.is_err() {
                                        break 'outer;
                                    }
                                } else {
                                    // havn't been actually sent; we must avoid flooding the sending queue
                                    packets1.push(packet.clone());
                                    continue;
                                }
                            },
                            Err(e) => {
                                log::warn!("Failed to retransmit packet: {:?}", e);
                                break 'outer;
                            }
                        }
                    }
                    packets = packets1;
                }
            }
        });
        Ok(())
    }


}

impl Sequence {
    pub fn new(total: u16) -> Self {
        Self {
            total,
            received: 0,
            fragments: (0..total).map(|_| Fragment::new()).collect(),
            last_active: get_timestamp() as u32,
        }
    }

    pub fn activate(&mut self) {
        self.last_active = get_timestamp() as u32;
    }

    pub fn gc(&mut self) {
        self.total = 0;
        self.received = 0;
        self.fragments.clear();
        self.last_active = 0;
    }

    pub fn with_data(&mut self, data: &[u8], chunk_size: usize) -> Result<(), MyError> {
        let mut data = data.to_vec();
        let mut fragments = Vec::new();
        while data.len() > 0 {
            let fragment = Fragment::new_with(data.drain(0..std::cmp::min(data.len(), chunk_size)).collect());
            fragments.push(fragment);
        }
        if fragments.len() == 0 {
            return Err(MyError::InvalidPacket("Empty data".to_string()));
        }
        if fragments.len() > u16::MAX as usize {
            return Err(MyError::InvalidPacket("Too many fragments".to_string()));
        }
        self.total = fragments.len() as u16;
        self.received = 0;
        self.fragments = fragments;
        Ok(())
    }

    pub fn len(&self) -> u16 {
        self.total
    }
}

impl Fragment {
    pub fn new() -> Self {
        Self {
            data: Vec::new(),
        }
    }

    pub fn new_with(data: Vec<u8>) -> Self {
        Self {
            data,
        }
    }

}
