use futures::{Future, FutureExt};
use std::collections::HashMap;
use std::collections::{BTreeMap, VecDeque};
use std::fmt::{self, Debug};
use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::task::{Context, Poll, Waker};
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::mpsc::{UnboundedReceiver as Receiver, UnboundedSender as Sender};
use tokio::sync::oneshot;
use tokio::time::Sleep;
use tracing::{debug, trace, warn};

use crate::EventOutgoing;
use crate::constants::{
    UDX_CLOCK_GRANULARITY_MS, UDX_HEADER_DATA, UDX_HEADER_END, UDX_HEADER_SACK, UDX_MAX_DATA_SIZE,
    UDX_MAX_TRANSMITS, UDX_MTU,
};
use crate::error::UdxError;
use crate::mutex::{Mutex, MutexGuard};
use crate::packet::{Header, IncomingPacket, Packet, PacketRef, PacketSet, read_u32_le};
use crate::socket::EventIncoming;
use crate::udp::UdpState;

const SSTHRESH: usize = 0xffff;
const MAX_SEGMENTS: usize = 10;
// const MAX_SEGMENTS: usize = 1;

#[derive(Debug, Default, Clone)]
pub struct StreamStats {
    pub tx_packets: usize,
    pub tx_bytes: usize,
    pub rx_bytes: usize,
    pub rx_packets: usize,
    pub inflight_packets: usize,
    pub inflight_bytes: usize,
}

#[derive(Debug)]
enum StreamState {
    Open,
    LocalClosed,
    RemoteClosed,
    BothClosed,
}

impl StreamState {
    fn close_local(&mut self) {
        *self = match self {
            StreamState::RemoteClosed | StreamState::BothClosed => StreamState::BothClosed,
            _ => StreamState::LocalClosed,
        }
    }
    fn close_remote(&mut self) {
        *self = match self {
            StreamState::LocalClosed | StreamState::BothClosed => StreamState::BothClosed,
            _ => StreamState::RemoteClosed,
        }
    }

    fn local_closed(&self) -> bool {
        matches!(self, StreamState::LocalClosed | StreamState::BothClosed)
    }

    fn remote_closed(&self) -> bool {
        matches!(self, StreamState::RemoteClosed | StreamState::BothClosed)
    }

    fn closed(&self) -> bool {
        self.local_closed() || self.remote_closed()
    }
}

pub struct StreamDriver(UdxStream);

impl Future for StreamDriver {
    type Output = ();
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut stream = self.0.lock("UdxStream::poll_drive");
        if stream.drive_waker.is_some() {
            stream.drive_waker = None;
        }

        if stream.closed_and_drained() {
            if let Some(tx) = stream.on_close.take() {
                tx.send(()).ok();
            }
            let _ = stream
                .send_tx
                .send(EventOutgoing::StreamDropped(stream.local_id));
            return Poll::Ready(());
        }

        let should_continue = stream.poll_drive(cx);

        let should_continue = match should_continue {
            Ok(should_continue) => should_continue,
            Err(err) => {
                stream.terminate(err);
                true
            }
        };
        if should_continue {
            drop(stream);
            cx.waker().wake_by_ref();
        } else {
            stream.drive_waker = Some(cx.waker().clone());
        }

        Poll::Pending
    }
}

#[derive(Clone)]
pub struct UdxStream(Arc<Mutex<UdxStreamInner>>);

impl UdxStream {
    pub(crate) fn connect(
        recv_rx: Receiver<EventIncoming>,
        send_tx: Sender<EventOutgoing>,
        udp_state: Arc<UdpState>,
        dest: SocketAddr,
        remote_id: u32,
        local_id: u32,
    ) -> Self {
        let rto = Duration::from_millis(1000);
        let stream = UdxStreamInner {
            recv_rx,
            remote_id,
            local_id,
            seq: 0,
            ack: 0,
            seq_flushed: 0,
            inflight: 0,
            cwnd: 2 * UDX_MTU,
            remote_acked: 0,
            read_cursor: 0,
            srtt: Duration::ZERO,
            rttvar: Duration::ZERO,
            rto_timeout: Box::pin(tokio::time::sleep(rto)),
            rto,
            send_tx,
            send_queue: VecDeque::new(),
            outgoing: HashMap::new(),
            incoming: BTreeMap::new(),
            remote_addr: dest,
            read_waker: None,
            write_waker: None,
            drive_waker: None,
            error: None,
            on_close: None,
            stats: Default::default(),
            state: StreamState::Open,
            udp_state,
            on_firewall: None,
            // out_of_order: 0,
        };
        let stream = UdxStream(Arc::new(Mutex::new(stream)));
        let driver = StreamDriver(stream.clone());
        tokio::task::spawn(driver);
        stream
    }

    pub fn close(&self) -> impl Future<Output = ()> {
        let mut stream = self.lock("UdxStream::close");
        stream.terminate(UdxError::close_graceful());
        let (tx, rx) = oneshot::channel();
        stream.on_close = Some(tx);
        drop(stream);

        rx.map(|_r| ())
    }

    pub fn closed(&self) -> bool {
        let stream = self.lock("UdxStream::close");
        stream.state.closed()
    }

    pub fn remote_addr(&self) -> SocketAddr {
        self.lock("UdxStream::remote_addr").remote_addr
    }

    pub fn stats(&self) -> StreamStats {
        (*self.lock("UdxStream::stats").stats()).clone()
    }

    pub(crate) fn lock(&self, reason: &'static str) -> MutexGuard<'_, UdxStreamInner> {
        self.0.lock(reason)
    }
}

impl AsyncRead for UdxStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let mut inner = self.0.lock("UdxStream::poll_read");
        Pin::new(&mut *inner).poll_read(cx, buf)
    }
}

impl AsyncWrite for UdxStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let mut inner = self.0.lock("UdxStream::poll_write");
        Pin::new(&mut *inner).poll_write(cx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
        Poll::Ready(Ok(()))
    }
}

pub(crate) struct UdxStreamInner {
    send_tx: Sender<EventOutgoing>,
    recv_rx: Receiver<EventIncoming>,

    incoming: BTreeMap<u32, IncomingPacket>,
    outgoing: HashMap<u32, Arc<Packet>>,
    send_queue: VecDeque<PacketRef>,

    remote_id: u32,
    local_id: u32,
    remote_addr: SocketAddr,

    seq: u32,          // highest seq we created and tried to send
    seq_flushed: u32,  // highest seq we flushed to the socket send queue
    ack: u32,          // highest ack we sent out
    remote_acked: u32, // highest ack we received from the remote
    inflight: usize,   // amount of bytes that are sent but not acked
    cwnd: usize,
    rto: Duration,
    rto_timeout: Pin<Box<Sleep>>,
    rttvar: Duration,
    srtt: Duration,

    // out_of_order: usize,
    read_cursor: u32,

    read_waker: Option<Waker>,
    write_waker: Option<Waker>,
    drive_waker: Option<Waker>,

    error: Option<UdxError>,

    on_close: Option<oneshot::Sender<()>>,
    on_firewall: Option<Box<dyn (Fn(SocketAddr) -> bool) + Send>>,

    stats: StreamStats,
    state: StreamState,

    udp_state: Arc<UdpState>,
}

impl Debug for UdxStreamInner {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("UdxStreamInner")
            .field("remote_id", &self.remote_id)
            .field("local_id", &self.local_id)
            .field("seq", &self.seq)
            .field("seq_flushed", &self.seq_flushed)
            .field("ack", &self.ack)
            .field("remote_acked", &self.remote_acked)
            .field("inflight", &self.inflight)
            .field("cwnd", &self.cwnd)
            .field("rto", &self.rto)
            .finish()
    }
}

// enum CloseReason {
//     Manual,
//     Dropped,
// }

impl Drop for UdxStream {
    fn drop(&mut self) {
        // Only the driver is left, shutdown.
        if Arc::strong_count(&self.0) <= 2 {
            self.0
                .lock("UdxStream::drop")
                .terminate(UdxError::close_graceful());
        }
    }
}

impl UdxStreamInner {
    fn create_header(&self, mut typ: u32) -> Header {
        if self.state.local_closed() {
            typ &= UDX_HEADER_END;
        }
        Header {
            stream_id: self.remote_id,
            typ,
            seq: self.seq,
            ack: self.ack,
            data_offset: 0,
            recv_win: u32::MAX,
        }
    }

    fn create_packet(&self, typ: u32, body: &[u8]) -> Packet {
        let header = self.create_header(typ);
        let dest = self.remote_addr;
        Packet::new(dest, header, body)
    }

    // fn close(&mut self, reason: CloseReason) {}

    fn terminate(&mut self, error: impl Into<UdxError>) {
        self.error = Some(error.into());
        self.state.close_local();
        self.send_state_packet();

        if let Some(waker) = self.read_waker.take() {
            waker.wake();
        }
        if let Some(waker) = self.write_waker.take() {
            waker.wake();
        }
        self.wake_driver();
    }

    fn closed_and_drained(&self) -> bool {
        self.error.is_some() && self.send_queue.is_empty() && self.outgoing.is_empty()
    }

    fn poll_drive(&mut self, cx: &mut Context<'_>) -> io::Result<bool> {
        self.poll_incoming(cx)?;
        self.poll_check_timeouts(cx)?;
        self.flush_waiting_packets();
        self.poll_transmit(cx);
        Ok(false)
    }

    fn wake_driver(&mut self) {
        if let Some(waker) = self.drive_waker.take() {
            waker.wake();
        }
    }

    fn poll_check_timeouts(&mut self, cx: &mut Context<'_>) -> io::Result<()> {
        match self.rto_timeout.as_mut().poll(cx) {
            Poll::Pending => return Ok(()),
            Poll::Ready(_) => {}
        }
        if self.remote_acked == self.seq {
            return Ok(());
        }
        // schedule next timeout.
        let next = tokio::time::Instant::now()
            .checked_add(2 * self.rto)
            .unwrap();
        self.rto_timeout.as_mut().reset(next);
        // shrink the window
        let old_cwnd = self.cwnd;
        self.cwnd = UDX_MTU.max(self.cwnd / 2);
        debug!(
            lid = self.local_id,
            rid = self.remote_id,
            "shrink cwnd from {} to {}, @seq {} @remote_acked {} @inflight {} @sql {}",
            old_cwnd,
            self.cwnd,
            self.seq,
            self.remote_acked,
            self.inflight,
            self.send_queue.len()
        );
        // Consider all packet losts - seems to be the simple consensus across different stream impls
        // which we like cause it is nice and simple to implement.
        for i in self.remote_acked..self.seq {
            if let Some(packet) = self.outgoing.get(&i) {
                if packet.waiting.load(Ordering::SeqCst) {
                    continue;
                }
                let may_retransmit = packet.transmits.fetch_update(
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                    |transmits| {
                        if transmits > UDX_MAX_TRANSMITS as usize {
                            None
                        } else {
                            Some(transmits + 1)
                        }
                    },
                );
                if may_retransmit.is_err() {
                    return Err(io::Error::new(
                        io::ErrorKind::TimedOut,
                        "Max retransmits reached without ack",
                    ));
                }
                packet.waiting.store(true, Ordering::SeqCst);
                self.inflight -= packet.len();
            }
        }

        // self.flush_waiting_packets();
        Ok(())
    }

    // pub fn really_send_data_packet(&mut self, packet: PacketRef) -> bool {
    //     packet.waiting.store(false, Ordering::SeqCst);
    //     self.inflight += packet.len();
    //     self.send_queue.push_back(packet);
    //     true
    // }

    pub fn flush_waiting_packets(&mut self) {
        // queue retransmits and waiting packets.
        let mut queued_retransmits = 0;
        for i in self.remote_acked..self.seq {
            if let Some(packet) = self.outgoing.get(&i) {
                if !packet.waiting.load(Ordering::SeqCst) {
                    continue;
                }
                if self.inflight + packet.len() > self.cwnd {
                    break;
                }
                queued_retransmits += 1;
                packet.waiting.store(false, Ordering::SeqCst);
                self.inflight += packet.len();
                self.send_queue.push_back(PacketRef::Shared(packet.clone()));
            }
        }
        if queued_retransmits > 0 {
            debug!(
                lid = self.local_id,
                rid = self.remote_id,
                "queue retransmits {} ({} to {}) rto {:?} cwnd {} inflight {} sql {}",
                queued_retransmits,
                self.remote_acked,
                self.seq,
                self.rto,
                self.cwnd,
                self.inflight,
                self.send_queue.len()
            );
        }
    }

    fn poll_transmit(&mut self, _cx: &mut Context<'_>) {
        let max_segments = self.udp_state.max_gso_segments().min(MAX_SEGMENTS);
        let mut segment_size = 0;
        let mut queue = Vec::new();
        while let Some(packet) = self.send_queue.pop_front() {
            if packet.buf.len() != segment_size || queue.len() == max_segments {
                if !queue.is_empty() {
                    let set = PacketSet::new(self.remote_addr, queue, segment_size);
                    queue = Vec::new();
                    if let Err(_err) = self.send_tx.send(EventOutgoing::Transmit(set)) {
                        unimplemented!();
                    }
                }
                segment_size = packet.buf.len();
            }
            queue.push(packet);
        }
        if !queue.is_empty() {
            let set = PacketSet::new(self.remote_addr, queue, segment_size);
            let len = set.len();
            if let Err(_err) = self.send_tx.send(EventOutgoing::Transmit(set)) {
                warn!(
                    "failed to send {} packets: send channel to socket closed",
                    len
                )
                // unimplemented!();
            }
        }
    }

    fn poll_incoming(&mut self, cx: &mut Context<'_>) -> io::Result<()> {
        let mut remote_ack = 0;
        loop {
            let next_event = Pin::new(&mut self.recv_rx).poll_recv(cx);
            match next_event {
                Poll::Pending => break,
                Poll::Ready(None) => {
                    return Err(io::Error::new(
                        io::ErrorKind::BrokenPipe,
                        "Socket driver future was dropped",
                    ));
                }
                Poll::Ready(Some(event)) => match event {
                    EventIncoming::Packet(packet) => {
                        remote_ack = remote_ack.max(packet.ack());
                        self.handle_incoming(packet);
                        // self.send_acks();
                    }
                },
            }
        }
        self.handle_incoming_max_remote_ack(remote_ack);
        Ok(())
    }

    // fn send_acks(&mut self) {
    //     let prev = self.ack;
    //     // trace!(
    //     //     lid = self.local_id,
    //     //     rid = self.remote_id,
    //     //     "START QUEUE ACKS @ {}",
    //     //     self.ack
    //     // );

    //     while self.incoming.contains_key(&self.ack) {
    //         self.ack += 1;
    //         self.send_state_packet();
    //     }
    //     if prev < self.ack {
    //         trace!(
    //             lid = self.local_id,
    //             rid = self.remote_id,
    //             "send acks from {} to {}",
    //             prev,
    //             self.ack
    //         );
    //     }
    // }

    fn handle_incoming_max_remote_ack(&mut self, ack: u32) {
        if ack <= self.remote_acked {
            return;
        }

        while self.remote_acked < ack {
            self.remote_acked += 1;
            let packet = self.outgoing.remove(&(self.remote_acked - 1));

            if let Some(packet) = packet {
                self.handle_remote_ack_for_packet(&packet);
            } else {
                // Received invalid ack (too high)
                tracing::warn!("received invalid ack (too high)");
            }
        }

        if (self.inflight + UDX_MTU) <= self.cwnd
            && let Some(waker) = self.write_waker.take()
        {
            waker.wake();
        }

        // reset rto, since things are moving forward.
        self.rto_timeout
            .as_mut()
            .reset(tokio::time::Instant::now().checked_add(self.rto).unwrap());
    }

    fn handle_remote_ack_for_packet(&mut self, packet: &Packet) {
        if !packet.waiting.load(Ordering::SeqCst) {
            self.inflight -= packet.buf.len();
        }
        self.stats.inflight_bytes -= packet.data_len();
        self.stats.inflight_packets -= 1;

        // recalculate timings
        if !packet.time_sent.is_empty() && packet.transmits.load(Ordering::SeqCst) == 1 {
            let rtt = packet.time_sent.elapsed();
            // First round trip time sample
            if self.srtt.is_zero() {
                self.srtt = rtt;
                self.rttvar = rtt / 2;
                self.rto = self.srtt + UDX_CLOCK_GRANULARITY_MS.max(4 * self.rttvar);
            } else {
                let delta = self.srtt.abs_diff(rtt);
                // RTTVAR <- (1 - beta) * RTTVAR + beta * |SRTT - R'| where beta is 1/4
                self.rttvar = (3 * self.rttvar + delta) / 4;
                // SRTT <- (1 - alpha) * SRTT + alpha * R' where alpha is 1/8
                self.srtt = (7 * self.srtt + rtt) / 8;
            }
            // RTO <- SRTT + max (G, K*RTTVAR) where K is 4 maxed with 1s
            self.rto = Duration::from_millis(1000)
                .max(self.srtt + UDX_CLOCK_GRANULARITY_MS.max(4 * self.rttvar));
        }

        // If this packet was queued for sending we need to remove it from the queue.
        packet.skip.store(true, Ordering::SeqCst);
    }

    fn handle_incoming_sacks(&mut self, packet: &IncomingPacket) {
        // TODO
        // let mut n = 0;
        let mut i = 0;
        let buf = &packet.buf[..];
        while i + 8 < buf.len() {
            let start = read_u32_le(&buf[i..(i + 4)]);
            i += 4;
            let end = read_u32_le(&buf[i..(i + 4)]);
            i += 4;
            for i in start..end {
                let packet = self.outgoing.remove(&i);
                if let Some(packet) = packet {
                    self.handle_remote_ack_for_packet(&packet);
                }
            }
        }
    }

    // fn process_packet
    fn handle_incoming(&mut self, packet: IncomingPacket) {
        trace!(
            lid = self.local_id,
            rid = self.remote_id,
            seq = self.seq,
            ack = self.ack,
            rack = self.remote_acked,
            "handle incoming typ {} seq {} ack {}",
            packet.header.typ,
            packet.header.seq,
            packet.header.ack
        );

        // check firewall.
        let pass = self
            .on_firewall
            .as_ref()
            .map(|on_firewall| on_firewall(packet.from))
            .unwrap_or(true);
        if !pass {
            return;
        }

        self.stats.rx_packets += 1;

        // TODO: Support relay
        // if (stream->relay_to) return relay_packet(stream, buf, buf_len, type, seq, ack);

        if packet.has_type(UDX_HEADER_SACK) {
            self.handle_incoming_sacks(&packet);
        }

        // done with header processing.

        let header = packet.header.clone();

        if packet.has_type(UDX_HEADER_END) {
            self.state.close_remote();
            self.terminate(UdxError::closed_by_remote());
        }

        // process incoming data
        if packet.has_type(UDX_HEADER_DATA) {
            self.stats.rx_bytes += packet.data_len();
            let seq = packet.seq();
            // ignore packets older than what we acked already
            if seq >= self.ack {
                self.incoming.insert(seq, packet);
            }

            // self.out_of_order += 1;

            // increase ack for in-order packets
            while self.incoming.contains_key(&self.ack) {
                self.ack += 1;
                // self.out_of_order -= 1;
            }

            // packet is next in line, wake the read waker.
            if seq <= self.ack
                && let Some(waker) = self.read_waker.take()
            {
                waker.wake();
            }
            self.send_state_packet();
        }

        // TODO: message packets
        // if packet.has_type(UDX_HEADER_MESSAGE) {
        //     self.handle_recv_message(packet)
        // }

        // check if received ack is out of bounds
        if header.ack > self.seq {
            return;
        }

        // let is_limited = self.inflight + 2 * UDX_MSS < self.cwnd * UDX_MSS;

        // congestion control..
        if header.ack > self.remote_acked {
            if self.cwnd < SSTHRESH {
                self.cwnd += UDX_MTU;
            } else {
                self.cwnd += ((UDX_MTU * UDX_MTU) / self.cwnd).max(1);
            }
        }
    }

    // fn ack_update(&mut self, acked: usize, is_limited: bool) {}

    fn read_next(&mut self, buf: &mut tokio::io::ReadBuf<'_>) -> io::Result<bool> {
        let mut did_read = false;
        while let Some(mut packet) = self.incoming.remove(&self.read_cursor) {
            let start = packet.read_offset;
            let len = packet.buf.len() - start;
            if len > buf.remaining() {
                let end = start + buf.remaining();
                buf.put_slice(&packet.buf[start..end]);
                packet.read_offset = end;
                self.incoming.insert(packet.header.seq, packet);
                break;
            }

            buf.put_slice(&packet.buf[start..]);
            did_read = true;
            self.read_cursor += 1;
        }
        Ok(did_read)
    }

    fn stats(&self) -> &StreamStats {
        &self.stats
    }

    fn send_state_packet(&mut self) {
        let mut typ = 0;
        if let Some(_error) = &self.error {
            typ |= UDX_HEADER_END;
        }
        let packet = self.create_packet(typ, &[]);
        self.send_queue.push_back(PacketRef::Owned(packet));
        self.wake_driver();
        self.stats.tx_packets += 1;
    }

    fn send_data_packet(&mut self, buf: &[u8]) -> usize {
        let len = buf.len().min(UDX_MAX_DATA_SIZE);
        if self.inflight + len > self.cwnd {
            return 0;
        }
        let packet = self.create_packet(UDX_HEADER_DATA, &buf[..len]);
        let packet = Arc::new(packet);

        self.seq += 1;

        self.stats.inflight_bytes += packet.data_len();
        self.stats.tx_bytes += packet.data_len();
        self.stats.tx_packets += 1;
        self.stats.inflight_packets += 1;

        self.outgoing.insert(packet.seq(), Arc::clone(&packet));
        // if self.send_queue.is_empty() {
        self.inflight += packet.buf.len();
        self.send_queue.push_back(PacketRef::Shared(packet));
        // } else {
        //     packet.waiting.store(true, Ordering::SeqCst);
        // }
        self.wake_driver();
        len
    }
}

impl AsyncWrite for UdxStreamInner {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        if let Some(error) = &self.error {
            return Poll::Ready(Err(error.clone().into()));
        }
        if buf.is_empty() {
            return Poll::Ready(Ok(0));
        }
        let mut written = 0;
        loop {
            if written >= buf.len() {
                return Poll::Ready(Ok(written));
            }
            let n = self.send_data_packet(&buf[written..]);
            if n == 0 {
                return if written == 0 {
                    self.write_waker = Some(cx.waker().clone());
                    Poll::Pending
                } else {
                    Poll::Ready(Ok(written))
                };
            }
            written += n;
        }
    }
    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
        Poll::Ready(Ok(()))
    }
}

impl AsyncRead for UdxStreamInner {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        // trace!(
        //     "poll read @ {} incoming len {}",
        //     self.ack,
        //     self.incoming.len()
        // );
        let did_read = self.read_next(buf)?;
        if !did_read {
            if let Some(error) = &self.error {
                return Poll::Ready(Err(error.clone().into()));
            }
            self.read_waker = Some(cx.waker().clone());
            Poll::Pending
        } else {
            Poll::Ready(Ok(()))
        }
    }
}

impl Debug for UdxStream {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let inner = self.0.lock("UdxStream::debug");
        write!(f, "{:#?}", *inner)
    }
}
