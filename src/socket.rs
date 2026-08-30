use bytes::BytesMut;
use futures::Future;
use std::collections::HashMap;
use std::collections::VecDeque;
use std::fmt;
use std::fmt::Debug;
use std::io;
use std::io::IoSliceMut;
use std::mem::MaybeUninit;
use std::net::IpAddr;
use std::net::Ipv4Addr;
use std::net::SocketAddr;
use std::net::ToSocketAddrs;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Waker;
use std::task::{Context, Poll};
use std::time::Duration;
use std::time::Instant;
use tokio::sync::mpsc::{self, UnboundedReceiver as Receiver, UnboundedSender as Sender};
use tokio::time::Sleep;
use tracing::{debug, trace};

use crate::constants::UDX_DEFAULT_TTL;
use crate::constants::UDX_HEADER_SIZE;
use crate::constants::UDX_MTU;
use crate::mutex::Mutex;
use crate::packet::{Dgram, Header, IncomingPacket, PacketSet};
use crate::stream::UdxStream;
use crate::transport::Transport;
use crate::udp::{BATCH_SIZE, RecvMeta, Transmit, UdpSocket, UdpState};

const MAX_LOOP: usize = 60;

const RECV_QUEUE_MAX_LEN: usize = 1024;

#[derive(Debug)]
pub(crate) enum EventIncoming {
    Packet(IncomingPacket),
}

#[derive(Debug)]
pub(crate) enum EventOutgoing {
    Transmit(PacketSet),
    TransmitDgram(Dgram),
    // TransmitOne(PacketRef),
    StreamDropped(u32),
}

#[derive(Debug)]
struct StreamHandle {
    recv_tx: Sender<EventIncoming>,
}

#[derive(Clone, Debug)]
pub struct UdxSocket(Arc<Mutex<UdxSocketInner>>);

impl std::ops::Deref for UdxSocket {
    type Target = Mutex<UdxSocketInner>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl UdxSocket {
    pub fn bind_rnd() -> io::Result<Self> {
        Self::bind_port(0)
    }
    pub fn bind_port(port: u16) -> io::Result<Self> {
        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port);
        Self::bind(addr)
    }
    // TODO FIXME this is not async but requires tokio running. Which will cause a runtime failure.
    // rm this depndence
    /// Create a socket on the given `addr`. Note `addr` is a *local* address normally it would
    /// look like `127.0.0.1:8080` which creates a socket on port `8080`. To connect to any random
    /// port pass `:0` as the port.
    pub fn bind<A: ToSocketAddrs>(addr: A) -> io::Result<Self> {
        Ok(Self::spawn_driving(UdxSocketInner::bind(addr)?))
    }

    /// Run udx over a [`Transport`] of your own rather than a kernel UDP socket.
    ///
    /// Everything above this point is unchanged: the socket, its streams, congestion
    /// control and retransmission all behave exactly as they do on a real socket. Only
    /// where the datagrams go is different.
    pub fn with_transport(transport: impl Transport) -> Self {
        Self::spawn_driving(UdxSocketInner::with_transport(Box::new(transport)))
    }

    fn spawn_driving(inner: UdxSocketInner) -> Self {
        let socket = Self(Arc::new(Mutex::new(inner)));
        let driver = SocketDriver(socket.clone());
        tokio::spawn(async {
            if let Err(e) = driver.await {
                tracing::error!("Socket I/O error: {}", e);
            }
        });
        socket
    }

    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.0.lock("UdxSocket::local_addr").socket.local_addr()
    }

    pub fn create_stream(&self, local_id: u32) -> io::Result<HalfOpenStreamHandle> {
        self.0.lock("UdxSocket::make_stream").streams.insert(
            local_id,
            MaybeOpenStream::HalfOpen(HalfOpenStream {
                socket: self.clone(),
                local_id,
                rx_messages: vec![],
            }),
        );
        Ok(HalfOpenStreamHandle {
            socket: self.clone(),
            local_id,
        })
    }
    pub fn connect(
        &self,
        dest: SocketAddr,
        local_id: u32,
        remote_id: u32,
    ) -> io::Result<UdxStream> {
        self.0
            .lock("UdxSocket::connect")
            .connect(dest, local_id, remote_id)
    }

    pub fn stats(&self) -> SocketStats {
        self.0.lock("UdxSocket::stats").stats.clone()
    }

    pub fn send(&self, dest: SocketAddr, buf: &[u8]) {
        self.send_with_ttl(dest, buf, UDX_DEFAULT_TTL)
    }

    /// Send a datagram with an explicit hop budget.
    ///
    /// See [`Dgram::with_ttl`]. Holepunching uses a low TTL to open a local NAT binding
    /// without the packet reaching the peer.
    pub fn send_with_ttl(&self, dest: SocketAddr, buf: &[u8], ttl: u8) {
        let dgram = Dgram::new(dest, buf.to_vec()).with_ttl(ttl);
        let ev = EventOutgoing::TransmitDgram(dgram);
        self.0.lock("UdxSocket::send").send_tx.send(ev).unwrap();
    }

    pub fn recv(&self) -> RecvFuture {
        RecvFuture(self.clone())
    }
}

pub struct RecvFuture(UdxSocket);
impl Future for RecvFuture {
    type Output = io::Result<(SocketAddr, Vec<u8>)>;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut socket = self.0.lock("UdxSocket::recv");
        if let Some(dgram) = socket.recv_dgrams.pop_front() {
            if !socket.recv_dgrams.is_empty() {
                cx.waker().wake_by_ref();
            }
            Poll::Ready(Ok((dgram.dest, dgram.buf)))
        } else {
            socket.recv_waker = Some(cx.waker().clone());
            Poll::Pending
        }
    }
}

impl Drop for UdxSocket {
    fn drop(&mut self) {
        // Only the driver is left, shutdown.
        if Arc::strong_count(&self.0) == 2 {
            let mut socket = self.0.lock("UdxSocket::drop");
            socket.has_refs = false;
            if let Some(waker) = socket.drive_waker.take() {
                waker.wake();
            }
        }
    }
}

pub struct SocketDriver(UdxSocket);

impl Future for SocketDriver {
    type Output = io::Result<()>;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut socket = self.0.lock("UdxSocket::poll_drive");
        let mut should_continue = false;
        should_continue |= socket.poll_recv(cx)?;
        if let Some(send_overflow_timer) = socket.send_overflow_timer.as_mut() {
            match send_overflow_timer.as_mut().poll(cx) {
                Poll::Pending => {}
                Poll::Ready(()) => {
                    log::warn!("send overflow timer clear!");
                    socket.send_overflow_timer = None;
                    should_continue = true;
                }
            }
        } else {
            should_continue |= socket.poll_transmit(cx)?;
        }

        if !should_continue && !socket.has_refs && socket.streams.is_empty() {
            return Poll::Ready(Ok(()));
        }
        if should_continue {
            drop(socket);
            cx.waker().wake_by_ref();
        } else {
            socket.drive_waker = Some(cx.waker().clone());
        }
        Poll::Pending
    }
}

#[derive(Debug)]
pub struct HalfOpenStreamHandle {
    socket: UdxSocket,
    local_id: u32,
}

impl HalfOpenStreamHandle {
    pub fn connect(self, dest: SocketAddr, remote_id: u32) -> io::Result<UdxStream> {
        let Some(MaybeOpenStream::HalfOpen(ds)) = self
            .socket
            .0
            .lock("HalfOpenStreamHandle::connect get stream")
            .streams
            .remove(&self.local_id)
        else {
            todo!()
        };
        let (stream, handle) = ds.connect(dest, remote_id)?;
        for event in ds.rx_messages {
            if let Err(_packet) = handle.recv_tx.send(event) {
                // stream dropped?
                todo!()
            }
        }
        self.socket
            .0
            .lock("HalfOpenStreamHandle:: put stream")
            .streams
            .insert(self.local_id, MaybeOpenStream::Open(handle));
        Ok(stream)
    }
}

#[derive(Debug)]
struct HalfOpenStream {
    socket: UdxSocket,
    local_id: u32,
    rx_messages: Vec<EventIncoming>,
}

impl HalfOpenStream {
    fn connect(&self, dest: SocketAddr, remote_id: u32) -> io::Result<(UdxStream, StreamHandle)> {
        let inner = self.socket.0.lock("DisconnectedStream::connect");
        let (recv_tx, recv_rx) = mpsc::unbounded_channel();
        let stream = UdxStream::connect(
            recv_rx,
            inner.send_tx.clone(),
            inner.udp_state.clone(),
            dest,
            remote_id,
            self.local_id,
        );
        // replay messages
        let handle = StreamHandle { recv_tx };
        Ok((stream, handle))
    }
}

#[derive(Debug)]
enum MaybeOpenStream {
    HalfOpen(HalfOpenStream),
    Open(StreamHandle),
}

pub struct UdxSocketInner {
    socket: Box<dyn Transport>,
    send_rx: Receiver<EventOutgoing>,
    send_tx: Sender<EventOutgoing>,
    streams: HashMap<u32, MaybeOpenStream>,
    outgoing_transmits: VecDeque<Transmit>,
    outgoing_packet_sets: VecDeque<PacketSet>,
    recv_buf: Option<Box<[u8]>>,
    udp_state: Arc<UdpState>,
    stats: SocketStats,
    has_refs: bool,
    drive_waker: Option<Waker>,

    send_overflow_timer: Option<Pin<Box<Sleep>>>,
    recv_dgrams: VecDeque<Dgram>,
    recv_waker: Option<Waker>,
}

impl fmt::Debug for UdxSocketInner {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("UdxSocketInner")
            .field("socket", &self.socket)
            .field("streams", &self.streams)
            .field("pending_transmits", &self.outgoing_transmits.len())
            .field("udp_state", &self.udp_state)
            .field("stats", &self.stats)
            .field("has_refs", &self.has_refs)
            .field("drive_waker", &self.drive_waker)
            .finish()
    }
}

#[derive(Default, Clone, Debug)]
pub struct SocketStats {
    tx_transmits: usize,
    tx_dgrams: usize,
    tx_bytes: usize,
    rx_bytes: usize,
    rx_dgrams: usize,
    tx_window_start: Option<Instant>,
    tx_window_bytes: usize,
    tx_window_dgrams: usize,
}

impl SocketStats {
    fn track_tx(&mut self, transmit: &Transmit) {
        self.tx_bytes += transmit.contents.len();
        self.tx_transmits += 1;
        self.tx_dgrams += transmit.num_segments();
        self.tx_window_bytes += transmit.contents.len();
        self.tx_window_dgrams += transmit.num_segments();
        if self.tx_window_start.is_none() {
            self.tx_window_start = Some(Instant::now());
        }
        if self.tx_window_start.as_ref().unwrap().elapsed() > Duration::from_millis(1000) {
            let elapsed = self
                .tx_window_start
                .as_ref()
                .unwrap()
                .elapsed()
                .as_secs_f32();
            trace!(
                "{} MB/s {} pps",
                self.tx_window_bytes as f32 / (1024. * 1024.) / elapsed,
                self.tx_window_dgrams as f32 / elapsed
            );
            self.tx_window_bytes = 0;
            self.tx_window_dgrams = 0;
            self.tx_window_start = Some(Instant::now());
        }
    }
}

impl UdxSocketInner {
    pub fn bind<A: ToSocketAddrs>(addr: A) -> io::Result<Self> {
        let socket = std::net::UdpSocket::bind(addr)?;
        Ok(Self::with_transport(Box::new(UdpSocket::from_std(socket)?)))
    }

    pub fn with_transport(socket: Box<dyn Transport>) -> Self {
        let (send_tx, send_rx) = mpsc::unbounded_channel();
        let recv_buf = vec![0; UDX_MTU * BATCH_SIZE];
        Self {
            // Batching is sized by what the transport says it can do, not by what the
            // platform can do, so a transport that is not a kernel socket is not handed
            // GSO transmits it would have to take apart again.
            udp_state: Arc::new(socket.udp_state()),
            socket,
            send_rx,
            send_tx,
            streams: HashMap::new(),
            recv_buf: Some(recv_buf.into()),
            outgoing_transmits: VecDeque::with_capacity(BATCH_SIZE),
            outgoing_packet_sets: VecDeque::with_capacity(BATCH_SIZE),
            stats: SocketStats::default(),
            has_refs: true,
            drive_waker: None,
            send_overflow_timer: None,
            recv_waker: None,
            recv_dgrams: VecDeque::new(),
        }
    }

    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.socket.local_addr()
    }

    pub fn connect(
        &mut self,
        dest: SocketAddr,
        local_id: u32,
        remote_id: u32,
    ) -> io::Result<UdxStream> {
        debug!(
            "UdxSocketInner::connect {} [{}] -> {} [{}])",
            self.local_addr().unwrap(),
            local_id,
            dest,
            remote_id
        );
        let (recv_tx, recv_rx) = mpsc::unbounded_channel();
        let stream = UdxStream::connect(
            recv_rx,
            self.send_tx.clone(),
            self.udp_state.clone(),
            dest,
            remote_id,
            local_id,
        );
        let handle = StreamHandle { recv_tx };
        self.streams.insert(local_id, MaybeOpenStream::Open(handle));
        Ok(stream)
    }

    fn poll_transmit(&mut self, cx: &mut Context<'_>) -> io::Result<bool> {
        let mut iters = 0;
        loop {
            iters += 1;
            let mut send_rx_pending = false;
            while self.outgoing_transmits.len() < BATCH_SIZE {
                match Pin::new(&mut self.send_rx).poll_recv(cx) {
                    Poll::Pending => {
                        send_rx_pending = true;
                        break;
                    }
                    Poll::Ready(None) => unreachable!(),
                    Poll::Ready(Some(event)) => match event {
                        EventOutgoing::StreamDropped(local_id) => {
                            let _ = self.streams.remove(&local_id);
                        }
                        EventOutgoing::TransmitDgram(dgram) => {
                            self.outgoing_transmits.push_back(dgram.into_transmit());
                        }
                        EventOutgoing::Transmit(packet_set) => {
                            let transmit = packet_set.to_transmit();
                            trace!("send {:?}", packet_set);
                            self.outgoing_transmits.push_back(transmit);
                            self.outgoing_packet_sets.push_back(packet_set);
                        }
                    },
                }
            }
            if self.outgoing_transmits.is_empty() {
                break Ok(false);
            }

            match self
                .socket
                .poll_send(&self.udp_state, cx, self.outgoing_transmits.as_slices().0)
            {
                Poll::Pending => break Ok(false),
                Poll::Ready(Err(err)) if err.kind() == io::ErrorKind::Interrupted => {
                    // Send overflow! Scale back write rate.
                    self.send_overflow_timer =
                        Some(Box::pin(tokio::time::sleep(Duration::from_millis(20))));
                    log::warn!("send overflow timer set!");
                    break Ok(false);
                }
                Poll::Ready(Err(err)) => break Err(err),
                Poll::Ready(Ok(n)) => {
                    for transmit in self.outgoing_transmits.drain(..n) {
                        self.stats.track_tx(&transmit);
                    }
                    // update packet sent time for data packets.
                    let n = n.min(self.outgoing_packet_sets.len());
                    for packet_set in self.outgoing_packet_sets.drain(..n) {
                        for packet in packet_set.iter_shared() {
                            packet.time_sent.set_now();
                        }
                    }
                }
            }
            if send_rx_pending {
                break Ok(false);
            }
            if iters > 0 {
                break Ok(true);
            }
        }
    }

    fn poll_recv(&mut self, cx: &mut Context<'_>) -> io::Result<bool> {
        let mut metas = [RecvMeta::default(); BATCH_SIZE];
        let mut recv_buf = self.recv_buf.take().unwrap();
        let mut iovs = unsafe { iovectors_from_buf::<BATCH_SIZE>(&mut recv_buf) };

        // process recv
        let mut iters = 0;
        let res = loop {
            iters += 1;
            if iters == MAX_LOOP {
                break Ok(true);
            }
            match self.socket.poll_recv(cx, &mut iovs, &mut metas) {
                Poll::Ready(Ok(msgs)) => {
                    for (meta, buf) in metas.iter().zip(iovs.iter()).take(msgs) {
                        let data: BytesMut = buf[0..meta.len].into();
                        if let Err(data) = self.process_packet(data, meta) {
                            // received invalid header. emit as message on socket.
                            // TODO: Remove the queue and invoke a poll handler directly?
                            self.on_recv_dgram(data, meta);
                        }
                    }
                }
                Poll::Pending => break Ok(false),
                Poll::Ready(Err(e)) => break Err(e),
            }
        };
        self.recv_buf = Some(recv_buf);
        res
    }

    fn on_recv_dgram(&mut self, data: BytesMut, meta: &RecvMeta) {
        if self.recv_dgrams.len() < RECV_QUEUE_MAX_LEN {
            self.recv_dgrams
                .push_back(Dgram::new(meta.addr, data.to_vec()));
            if let Some(waker) = self.recv_waker.take() {
                waker.wake()
            }
        } else {
            drop(data)
        }
    }

    fn process_packet(&mut self, mut data: BytesMut, meta: &RecvMeta) -> Result<(), BytesMut> {
        let local_addr = self.local_addr().unwrap();
        let len = data.len();
        self.stats.rx_bytes += len;
        self.stats.rx_dgrams += 1;

        // try to decode the udx header
        let header = match Header::from_bytes(&data) {
            Ok(header) => header,
            Err(_) => return Err(data),
        };
        let stream_id = header.stream_id;
        trace!(
            to = stream_id,
            "[{}] recv from :{} typ {} seq {} ack {} len {}",
            local_addr.port(),
            meta.addr.port(),
            header.typ,
            header.seq,
            header.ack,
            len
        );
        match self.streams.get_mut(&stream_id) {
            Some(stream) => {
                let _ = data.split_to(UDX_HEADER_SIZE);
                let incoming = IncomingPacket {
                    header,
                    buf: data.into(),
                    read_offset: 0,
                    from: meta.addr,
                };
                let event = EventIncoming::Packet(incoming);
                match stream {
                    MaybeOpenStream::Open(handle) => {
                        if let Err(_p) = handle.recv_tx.send(event) {
                            // stream was dropped.
                            // remove stream?
                            self.streams.remove(&stream_id);
                        }
                    }
                    MaybeOpenStream::HalfOpen(ds) => {
                        ds.rx_messages.push(event);
                    }
                }
            }
            None => {
                // received packet for nonexisting stream.
                return Err(data);
            }
        }
        Ok(())
    }
}

pub enum SocketEvent {
    UnknownStream,
}

// Create an array of IO vectors from a buffer.
// Safety: buf has to be longer than N. You may only read from slices that have been written to.
// Taken from: quinn/src/endpoint.rs
unsafe fn iovectors_from_buf<const N: usize>(buf: &mut [u8]) -> [IoSliceMut<'_>; N] {
    let mut iovs = MaybeUninit::<[IoSliceMut; N]>::uninit();
    buf.chunks_mut(buf.len() / N)
        .enumerate()
        .for_each(|(i, buf)| {
            unsafe {
                iovs.as_mut_ptr()
                    .cast::<IoSliceMut>()
                    .add(i)
                    .write(IoSliceMut::new(buf))
            };
        });
    unsafe { iovs.assume_init() }
}
