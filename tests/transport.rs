//! A [`UdxSocket`] must be able to run on a caller-supplied [`Transport`] rather than a
//! kernel socket, with everything above the datagram layer behaving identically.
//!
//! This lives in `tests/` deliberately: it can only use the crate's public API, so it also
//! checks that implementing `Transport` out of tree is actually possible, i.e. that the
//! trait and the `udp` types it is written in terms of are all exported.

use std::io::{self, IoSliceMut};
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::task::{Context, Poll};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use udx::udp::{RecvMeta, Transmit, UdpSocket, UdpState};
use udx::{Transport, UdxSocket};

/// A transport that forwards to a real socket and counts what passes through.
///
/// Deliberately not a simulated network: the point here is to prove the seam carries live
/// traffic, so the datagrams are real and only the path they take through the code differs.
#[derive(Debug)]
struct CountingTransport {
    inner: UdpSocket,
    sent: Arc<AtomicUsize>,
    received: Arc<AtomicUsize>,
}

impl CountingTransport {
    fn bind(sent: Arc<AtomicUsize>, received: Arc<AtomicUsize>) -> io::Result<Self> {
        let std_sock = std::net::UdpSocket::bind("127.0.0.1:0")?;
        Ok(Self {
            inner: UdpSocket::from_std(std_sock)?,
            sent,
            received,
        })
    }
}

impl Transport for CountingTransport {
    fn poll_send(
        &mut self,
        state: &UdpState,
        cx: &mut Context<'_>,
        transmits: &[Transmit],
    ) -> Poll<io::Result<usize>> {
        let res = self.inner.poll_send(state, cx, transmits);
        if let Poll::Ready(Ok(n)) = &res {
            self.sent.fetch_add(*n, Ordering::SeqCst);
        }
        res
    }

    fn poll_recv(
        &self,
        cx: &mut Context<'_>,
        bufs: &mut [IoSliceMut<'_>],
        meta: &mut [RecvMeta],
    ) -> Poll<io::Result<usize>> {
        let res = self.inner.poll_recv(cx, bufs, meta);
        if let Poll::Ready(Ok(n)) = &res {
            self.received.fetch_add(*n, Ordering::SeqCst);
        }
        res
    }

    fn local_addr(&self) -> io::Result<SocketAddr> {
        self.inner.local_addr()
    }

    /// What a non-kernel transport would report, so the sender never builds a GSO transmit.
    fn udp_state(&self) -> UdpState {
        UdpState::with_max_gso_segments(1)
    }
}

/// Records whether the sender ever built a multi-segment (GSO) transmit, and reports a
/// caller-chosen GSO limit so the test can drive both sides of that behaviour.
#[derive(Debug)]
struct GsoProbeTransport {
    inner: UdpSocket,
    gso_limit: usize,
    segmented: Arc<AtomicBool>,
}

impl GsoProbeTransport {
    fn bind(gso_limit: usize, segmented: Arc<AtomicBool>) -> io::Result<Self> {
        let std_sock = std::net::UdpSocket::bind("127.0.0.1:0")?;
        Ok(Self {
            inner: UdpSocket::from_std(std_sock)?,
            gso_limit,
            segmented,
        })
    }
}

impl Transport for GsoProbeTransport {
    fn poll_send(
        &mut self,
        state: &UdpState,
        cx: &mut Context<'_>,
        transmits: &[Transmit],
    ) -> Poll<io::Result<usize>> {
        if transmits.iter().any(|t| t.segment_size.is_some()) {
            self.segmented.store(true, Ordering::SeqCst);
        }
        self.inner.poll_send(state, cx, transmits)
    }

    fn poll_recv(
        &self,
        cx: &mut Context<'_>,
        bufs: &mut [IoSliceMut<'_>],
        meta: &mut [RecvMeta],
    ) -> Poll<io::Result<usize>> {
        self.inner.poll_recv(cx, bufs, meta)
    }

    fn local_addr(&self) -> io::Result<SocketAddr> {
        self.inner.local_addr()
    }

    fn udp_state(&self) -> UdpState {
        UdpState::with_max_gso_segments(self.gso_limit)
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn datagrams_flow_both_ways_over_a_custom_transport() -> io::Result<()> {
    let sent = Arc::new(AtomicUsize::new(0));
    let received = Arc::new(AtomicUsize::new(0));

    let a = UdxSocket::with_transport(CountingTransport::bind(sent.clone(), received.clone())?);
    let b = UdxSocket::bind("127.0.0.1:0")?;
    let (a_addr, b_addr) = (a.local_addr()?, b.local_addr()?);

    // out through the custom transport's poll_send
    a.send(b_addr, b"ping");
    let (from, msg) = b.recv().await?;
    assert_eq!(msg, b"ping");
    assert_eq!(from, a_addr);

    // and back in through its poll_recv
    b.send(a_addr, b"pong");
    let (from, msg) = a.recv().await?;
    assert_eq!(msg, b"pong");
    assert_eq!(from, b_addr);

    assert!(sent.load(Ordering::SeqCst) > 0, "poll_send was never called");
    assert!(
        received.load(Ordering::SeqCst) > 0,
        "poll_recv was never called"
    );
    Ok(())
}

/// Push enough stream traffic through a socket to give the sender a chance to batch, and
/// report whether the transport was ever handed a multi-segment (GSO) transmit.
async fn saw_segmented_transmit(gso_limit: usize) -> io::Result<bool> {
    let segmented = Arc::new(AtomicBool::new(false));
    let transport = GsoProbeTransport::bind(gso_limit, segmented.clone())?;

    let a = UdxSocket::with_transport(transport);
    let b = UdxSocket::bind("127.0.0.1:0")?;
    let mut stream_a = a.connect(b.local_addr()?, 1, 2)?;
    let mut stream_b = b.connect(a.local_addr()?, 2, 1)?;

    const LEN: usize = 1 << 20;
    let reader = tokio::spawn(async move {
        let mut sink = vec![0u8; LEN];
        stream_b.read_exact(&mut sink).await
    });
    stream_a.write_all(&vec![7u8; LEN]).await?;
    reader.await.expect("reader panicked")?;

    Ok(segmented.load(Ordering::SeqCst))
}

/// The socket must batch according to what the transport reports, not what the platform
/// supports. Without this a simulated transport is handed GSO transmits carrying several
/// datagrams under one header, which it would have to take apart again.
///
/// The second half is the control: it shows the first assertion is not passing merely
/// because nothing would have been segmented anyway.
#[tokio::test(flavor = "multi_thread")]
async fn transport_dictates_the_gso_limit() -> io::Result<()> {
    assert!(
        !saw_segmented_transmit(1).await?,
        "transport reported a GSO limit of 1 but was handed a segmented transmit"
    );

    let platform_limit = UdpState::new().max_gso_segments();
    if platform_limit > 1 {
        assert!(
            saw_segmented_transmit(platform_limit).await?,
            "no segmentation even at the platform limit of {platform_limit}, so the check \
             above proves nothing on this machine"
        );
    }
    Ok(())
}
