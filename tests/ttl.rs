//! The hop budget passed to [`UdxSocket::send_with_ttl`] has to reach the IP header, not
//! just the `Transmit` struct. Holepunching depends on it: the first punch rounds are sent
//! at a low TTL so they open the local NAT binding but expire before reaching the peer.
//!
//! There is no receive-side TTL in the library (nothing in udx or hyperdht reads one), so
//! these tests enable `IP_RECVTTL` on their own socket and read the control message by hand.

#![cfg(unix)]

use std::io;
use std::mem;
use std::net::{SocketAddr, UdpSocket as StdUdpSocket};
use std::os::fd::AsRawFd;
use std::time::Duration;

use udx::UdxSocket;

/// Bind a plain socket that reports the TTL of each datagram it receives.
fn ttl_reporting_socket() -> io::Result<StdUdpSocket> {
    let sock = StdUdpSocket::bind("127.0.0.1:0")?;
    // So a failure shows up as a failed assertion rather than a hung test.
    sock.set_read_timeout(Some(Duration::from_secs(5)))?;

    let on: libc::c_int = 1;
    let rc = unsafe {
        libc::setsockopt(
            sock.as_raw_fd(),
            libc::IPPROTO_IP,
            libc::IP_RECVTTL,
            &on as *const _ as _,
            mem::size_of_val(&on) as _,
        )
    };
    if rc == -1 {
        return Err(io::Error::last_os_error());
    }
    Ok(sock)
}

/// Receive one datagram along with the TTL it arrived with.
///
/// Note the asymmetry: the socket option that enables this is `IP_RECVTTL`, but the control
/// message the kernel delivers is tagged `IP_TTL`.
fn recv_with_ttl(sock: &StdUdpSocket) -> io::Result<(Vec<u8>, u8)> {
    let mut buf = [0u8; 1024];
    let mut iov = libc::iovec {
        iov_base: buf.as_mut_ptr() as *mut _,
        iov_len: buf.len(),
    };
    let mut ctrl = [0u8; 256];
    let mut hdr: libc::msghdr = unsafe { mem::zeroed() };
    hdr.msg_iov = &mut iov;
    hdr.msg_iovlen = 1;
    hdr.msg_control = ctrl.as_mut_ptr() as *mut _;
    hdr.msg_controllen = ctrl.len() as _;

    let n = unsafe { libc::recvmsg(sock.as_raw_fd(), &mut hdr, 0) };
    if n < 0 {
        return Err(io::Error::last_os_error());
    }

    let mut ttl = None;
    unsafe {
        let mut cmsg = libc::CMSG_FIRSTHDR(&hdr);
        while !cmsg.is_null() {
            if (*cmsg).cmsg_level == libc::IPPROTO_IP && (*cmsg).cmsg_type == libc::IP_TTL {
                ttl = Some(std::ptr::read_unaligned(
                    libc::CMSG_DATA(cmsg) as *const libc::c_int
                ));
            }
            cmsg = libc::CMSG_NXTHDR(&hdr, cmsg);
        }
    }

    let ttl = ttl.ok_or_else(|| io::Error::other("no IP_TTL control message on the datagram"))?;
    Ok((buf[..n as usize].to_vec(), ttl as u8))
}

/// A low TTL has to survive the trip from the public API down into the IP header. Loopback
/// does not decrement, so we read the value that arrived rather than watch it expire.
#[tokio::test(flavor = "multi_thread")]
async fn send_with_ttl_reaches_the_ip_header() -> io::Result<()> {
    let rx = ttl_reporting_socket()?;
    let rx_addr: SocketAddr = rx.local_addr()?;

    let tx = UdxSocket::bind("127.0.0.1:0")?;
    tx.send_with_ttl(rx_addr, b"punch", 5);

    let (payload, ttl) = recv_with_ttl(&rx)?;
    assert_eq!(payload, b"punch");
    assert_eq!(ttl, 5, "low TTL did not reach the wire");
    Ok(())
}

/// The plain `send` path must be unaffected by the TTL plumbing.
#[tokio::test(flavor = "multi_thread")]
async fn plain_send_uses_the_default_ttl() -> io::Result<()> {
    let rx = ttl_reporting_socket()?;
    let rx_addr: SocketAddr = rx.local_addr()?;

    let tx = UdxSocket::bind("127.0.0.1:0")?;
    tx.send(rx_addr, b"hello");

    let (payload, ttl) = recv_with_ttl(&rx)?;
    assert_eq!(payload, b"hello");
    assert_eq!(ttl, 64, "default send should use UDX_DEFAULT_TTL");
    Ok(())
}
