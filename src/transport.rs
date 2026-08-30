//! The datagram layer a [`UdxSocket`](crate::UdxSocket) runs on.
//!
//! Normally this is a kernel UDP socket, which is the blanket impl below. Providing your own
//! lets udx run over something else entirely: a simulated network for deterministic tests,
//! an in-process loopback, a recording proxy.
//!
//! The trait is deliberately the same shape as the platform socket it abstracts, so the real
//! implementation is pure forwarding and costs nothing beyond one virtual call per batch. A
//! batch here is up to [`BATCH_SIZE`](crate::udp::BATCH_SIZE) datagrams moved by a single
//! `sendmmsg`/`recvmmsg`, so the dispatch is amortised over the syscall.

use std::{
    fmt::Debug,
    io::{self, IoSliceMut},
    net::SocketAddr,
    task::{Context, Poll},
};

use crate::udp::{RecvMeta, Transmit, UdpSocket, UdpState};

/// A datagram transport a socket can send and receive on.
///
/// `Send + 'static` because [`UdxSocket::bind`](crate::UdxSocket::bind) drives the socket
/// from a spawned task. `Debug` because the socket's own `Debug` prints its transport.
pub trait Transport: Debug + Send + 'static {
    /// Send as many of `transmits` as the transport will accept, returning how many it took.
    ///
    /// A `Transmit` whose `segment_size` is `Some` carries several datagrams that share one
    /// header, and counts as one transmit. Report a `max_gso_segments` of 1 from
    /// [`udp_state`](Self::udp_state) to be sure you never see one.
    fn poll_send(
        &mut self,
        state: &UdpState,
        cx: &mut Context<'_>,
        transmits: &[Transmit],
    ) -> Poll<io::Result<usize>>;

    /// Receive up to `bufs.len()` datagrams, writing one [`RecvMeta`] per datagram.
    fn poll_recv(
        &self,
        cx: &mut Context<'_>,
        bufs: &mut [IoSliceMut<'_>],
        meta: &mut [RecvMeta],
    ) -> Poll<io::Result<usize>>;

    /// The local address datagrams appear to come from.
    fn local_addr(&self) -> io::Result<SocketAddr>;

    /// The capabilities the sender should assume when batching for this transport.
    ///
    /// Defaults to probing the platform, which is right for a kernel socket. A transport
    /// that is not a kernel socket should return
    /// [`UdpState::with_max_gso_segments`] instead.
    fn udp_state(&self) -> UdpState {
        UdpState::new()
    }
}

impl Transport for UdpSocket {
    fn poll_send(
        &mut self,
        state: &UdpState,
        cx: &mut Context<'_>,
        transmits: &[Transmit],
    ) -> Poll<io::Result<usize>> {
        UdpSocket::poll_send(self, state, cx, transmits)
    }

    fn poll_recv(
        &self,
        cx: &mut Context<'_>,
        bufs: &mut [IoSliceMut<'_>],
        meta: &mut [RecvMeta],
    ) -> Poll<io::Result<usize>> {
        UdpSocket::poll_recv(self, cx, bufs, meta)
    }

    fn local_addr(&self) -> io::Result<SocketAddr> {
        UdpSocket::local_addr(self)
    }
}
