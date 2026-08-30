use std::fmt;
use std::net::{IpAddr, SocketAddr};

/// An outgoing packet
pub struct Transmit {
    /// The socket this datagram should be sent to
    pub destination: SocketAddr,
    /// Explicit congestion notification bits to set on the packet
    pub ecn: Option<EcnCodepoint>,
    /// Contents of the datagram
    pub contents: Vec<u8>,
    /// The segment size if this transmission contains multiple datagrams.
    /// This is `None` if the transmit only contains a single datagram
    pub segment_size: Option<usize>,
    /// Optional source IP address for the datagram
    pub src_ip: Option<IpAddr>,
    /// Number of hops before death
    pub ttl: u8,
}

impl Transmit {
    pub fn num_segments(&self) -> usize {
        match self.segment_size {
            None => 1,
            Some(size) => self.contents.len() / size,
        }
    }
}

impl fmt::Debug for Transmit {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Transmit")
            .field("destination", &self.destination)
            .field("ecn", &self.ecn)
            .field("contents", &format!("<buf len {}>", self.contents.len()))
            .field("segment_size", &self.segment_size)
            .field("src_ip", &self.src_ip)
            .field("ttl", &self.ttl)
            .finish()
    }
}

/// Explicit congestion notification codepoint
#[repr(u8)]
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum EcnCodepoint {
    #[doc(hidden)]
    Ect0 = 0b10,
    #[doc(hidden)]
    Ect1 = 0b01,
    #[doc(hidden)]
    Ce = 0b11,
}

impl EcnCodepoint {
    /// Create new object from the given bits
    pub fn from_bits(x: u8) -> Option<Self> {
        use self::EcnCodepoint::*;
        Some(match x & 0b11 {
            0b10 => Ect0,
            0b01 => Ect1,
            0b11 => Ce,
            _ => {
                return None;
            }
        })
    }
}
