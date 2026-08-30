use atomic_instant::AtomicInstant;
use bytes::{BufMut, Bytes};
use std::fmt::{self, Debug};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::{io, net::SocketAddr, sync::atomic::AtomicUsize};
use udx_udp::Transmit;

use crate::constants::{UDX_DEFAULT_TTL, UDX_HEADER_SIZE, UDX_MAGIC_BYTE, UDX_VERSION};

#[derive(Debug)]
pub struct Dgram {
    pub buf: Vec<u8>,
    pub dest: SocketAddr,
    /// Hop budget to send this datagram with.
    ///
    /// Only meaningful on the outgoing path. `Dgram` is also used to carry received
    /// datagrams, and those keep the default here because we don't read the TTL off
    /// incoming packets.
    pub ttl: u8,
}

impl Dgram {
    pub fn new(dest: SocketAddr, buf: Vec<u8>) -> Self {
        Self {
            dest,
            buf,
            ttl: UDX_DEFAULT_TTL,
        }
    }
    /// Send with a hop budget other than [`UDX_DEFAULT_TTL`].
    ///
    /// Holepunching needs this: its first punch rounds go out at a low TTL so they open the
    /// local NAT binding but expire in transit, before the remote's firewall sees an
    /// unsolicited inbound packet.
    pub fn with_ttl(mut self, ttl: u8) -> Self {
        self.ttl = ttl;
        self
    }
    pub fn into_transmit(self) -> Transmit {
        Transmit {
            segment_size: None,
            destination: self.dest,
            ecn: None,
            src_ip: None,
            contents: self.buf,
            ttl: self.ttl,
        }
    }
}

#[derive(Debug)]
pub(crate) enum PacketRef {
    Owned(Packet),
    Shared(Arc<Packet>),
}

// invariant: all packets need to have the same size if segment_size is set
// invariant: may not be larger than max_segments as reported from usp_state
pub struct PacketSet {
    dest: SocketAddr,
    segment_size: Option<usize>,
    pub(crate) packets: Vec<PacketRef>,
}

impl fmt::Debug for PacketSet {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let packet_debug = self
            .packets
            .iter()
            .map(|p| {
                format!(
                    "i:{} t:{} s:{} a:{} l:{}",
                    p.header.stream_id,
                    p.header.typ,
                    p.header.seq,
                    p.header.ack,
                    p.data_len()
                )
            })
            .collect::<Vec<_>>()
            .join(", ");
        f.debug_struct("PacketSet")
            .field("dest", &self.dest)
            .field("packet_count", &self.packets.len())
            .field("packets", &packet_debug)
            .field("segment_size", &self.segment_size)
            .finish()
    }
}

impl PacketSet {
    pub(crate) fn new(dest: SocketAddr, packets: Vec<PacketRef>, segment_size: usize) -> Self {
        Self {
            dest,
            packets,
            segment_size: Some(segment_size),
        }
    }

    pub fn len(&self) -> usize {
        self.packets.len()
    }

    pub fn iter_shared(&self) -> impl IntoIterator<Item = &Packet> {
        self.packets.iter().filter_map(|packet| match packet {
            PacketRef::Shared(packet) => Some(packet.as_ref()),
            _ => None,
        })
    }

    // pub fn new_single(packet: PacketRef) -> Self {
    //     Self {
    //         dest: packet.dest,
    //         packets: vec![packet],
    //         segment_size: None,
    //     }
    // }
    pub fn to_transmit(&self) -> Transmit {
        match self.segment_size {
            None => {
                assert!(self.packets.len() == 1);
                self.packets.first().unwrap().to_transmit()
            }
            Some(segment_size) => {
                // assert!(self.packets.len() <= max_segments);
                // let segments = self.packets.len().min(max_segments);
                let segments = self.packets.len();
                let size = segments * segment_size;
                let mut buf = Vec::with_capacity(size);
                for packet in self.packets.iter() {
                    if !packet.skip.load(Ordering::SeqCst) {
                        buf.put_slice(packet.buf.as_slice());
                    }
                }
                // for j in 0..segments {
                //     let packet = self.packets.get(j).unwrap();
                //     buf.put_slice(packet.buf.as_slice());
                // }

                Transmit {
                    destination: self.dest,
                    ecn: None,
                    src_ip: None,
                    contents: buf,
                    segment_size: match segments {
                        1 => None,
                        _ => Some(segment_size),
                    },
                    ttl: UDX_DEFAULT_TTL,
                }
                // self
                // .packets
                // .iter()
                // .map(|packet| packet.to_transmit())
                // .collect(),
                // let mut transmits = VecDeque::new();
                // queue_transmits(
                //     &mut transmits,
                //     &self.packets,
                //     segment_size,
                //     max_segments,
                //     self.dest,
                // );
                // transmits
            }
        }
    }
}

impl std::ops::Deref for PacketRef {
    type Target = Packet;
    fn deref(&self) -> &Self::Target {
        match self {
            PacketRef::Owned(packet) => packet,
            PacketRef::Shared(packet) => packet,
        }
    }
}

pub enum PacketBuf {
    Data(Vec<u8>),
    HeaderOnly([u8; 20]),
}
impl PacketBuf {
    pub fn as_slice(&self) -> &[u8] {
        match self {
            Self::Data(vec) => &vec[..],
            Self::HeaderOnly(array) => &array[..],
        }
    }
    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        match self {
            Self::Data(vec) => &mut vec[..],
            Self::HeaderOnly(array) => &mut array[..],
        }
    }
    pub fn len(&self) -> usize {
        self.as_slice().len()
    }

    pub fn into_vec(self) -> Vec<u8> {
        match self {
            Self::Data(buf) => buf,
            Self::HeaderOnly(buf) => buf.into(),
        }
    }
}

pub struct Packet {
    pub waiting: AtomicBool,
    pub skip: AtomicBool,
    pub time_sent: AtomicInstant,
    pub transmits: AtomicUsize,
    pub dest: SocketAddr,
    pub header: Header,
    pub buf: PacketBuf,
}

impl fmt::Debug for Packet {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Packet")
            .field("skip", &self.skip)
            .field("transmits", &self.transmits)
            .field("dest", &self.dest)
            .field("header", &self.header)
            .field("buf(len)", &self.buf.len())
            .finish()
    }
}

impl From<Packet> for Transmit {
    fn from(packet: Packet) -> Self {
        Transmit {
            ecn: None,
            src_ip: None,
            destination: packet.dest,
            segment_size: None,
            contents: packet.buf.into_vec(),
            ttl: UDX_DEFAULT_TTL,
        }
    }
}

impl Packet {
    pub fn new(dest: SocketAddr, header: Header, body: &[u8]) -> Self {
        let mut buf = if body.is_empty() {
            PacketBuf::HeaderOnly([0u8; 20])
        } else {
            let len = UDX_HEADER_SIZE + body.len();
            PacketBuf::Data(vec![0u8; len])
        };
        header.encode(&mut buf.as_mut_slice()[..UDX_HEADER_SIZE]);
        if !body.is_empty() {
            buf.as_mut_slice()[UDX_HEADER_SIZE..].copy_from_slice(body);
        }
        Self {
            skip: AtomicBool::new(false),
            waiting: AtomicBool::new(false),
            time_sent: AtomicInstant::empty(),
            transmits: AtomicUsize::new(0),
            dest,
            header,
            buf,
        }
    }

    pub fn seq(&self) -> u32 {
        self.header.seq
    }

    pub fn len(&self) -> usize {
        self.buf.len()
    }

    pub fn data_len(&self) -> usize {
        self.buf.len().saturating_sub(UDX_HEADER_SIZE)
    }

    fn to_transmit(&self) -> Transmit {
        Transmit {
            ecn: None,
            src_ip: None,
            destination: self.dest,
            segment_size: None,
            contents: self.buf.as_slice().to_vec(),
            ttl: UDX_DEFAULT_TTL,
        }
    }
}

pub(crate) struct IncomingPacket {
    pub header: Header,
    pub buf: Bytes,
    pub read_offset: usize,
    pub from: SocketAddr,
}

impl fmt::Debug for IncomingPacket {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "IncomingPacket(header {:?}, buf len {})",
            self.header,
            self.buf.len()
        )
    }
}

impl IncomingPacket {
    pub fn ack(&self) -> u32 {
        self.header.ack
    }
    pub fn seq(&self) -> u32 {
        self.header.seq
    }
    pub fn has_type(&self, typ: u32) -> bool {
        self.header.typ & typ != 0
    }
    pub fn data_len(&self) -> usize {
        self.buf.len()
    }
}

#[derive(Debug, Clone)]
pub struct Header {
    pub typ: u32,
    pub data_offset: usize,
    pub stream_id: u32,
    pub recv_win: u32,
    pub seq: u32,
    pub ack: u32,
}

impl Header {
    const SIZE: usize = UDX_HEADER_SIZE;
    // pub fn has_typ(&self, typ: u32) -> bool {
    //     self.typ & typ != 0
    // }
    pub fn from_bytes(buf: &[u8]) -> io::Result<Self> {
        if buf.len() < UDX_HEADER_SIZE || buf[0] != UDX_MAGIC_BYTE || buf[1] != UDX_VERSION {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "Bad header"));
        }

        let typ = buf[2] as u32;
        let data_offset = buf[3];
        let local_id = read_u32_le(&buf[4..8]);
        let recv_win = read_u32_le(&buf[8..12]);
        let seq = read_u32_le(&buf[12..16]);
        let ack = read_u32_le(&buf[16..20]);
        Ok(Self {
            typ,
            data_offset: data_offset as usize,
            recv_win,
            stream_id: local_id,
            seq,
            ack,
        })
    }

    // pub fn to_vec(&self) -> Vec<u8> {
    //     let mut buf = vec![0u8; Self::SIZE];
    //     self.encode(&mut buf);
    //     buf
    // }

    pub fn encode(&self, buf: &mut [u8]) -> bool {
        if buf.len() < Self::SIZE {
            return false;
        }
        buf[0] = UDX_MAGIC_BYTE;
        buf[1] = UDX_VERSION;
        buf[2..3].copy_from_slice(&(self.typ as u8).to_le_bytes());
        buf[3..4].copy_from_slice(&(self.data_offset as u8).to_le_bytes());
        buf[4..8].copy_from_slice(&self.stream_id.to_le_bytes());
        buf[8..12].copy_from_slice(&self.recv_win.to_le_bytes());
        buf[12..16].copy_from_slice(&self.seq.to_le_bytes());
        buf[16..20].copy_from_slice(&self.ack.to_le_bytes());
        true
    }
}

pub fn read_u32_le(buf: &[u8]) -> u32 {
    u32::from_le_bytes(buf.try_into().unwrap())
}
