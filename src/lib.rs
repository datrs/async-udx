mod constants;
mod error;
mod mutex;
mod packet;
mod socket;
mod stream;
mod transport;
pub use constants::UDX_DATA_MTU;
pub use error::*;
pub use socket::*;
pub use stream::*;
pub use transport::Transport;

/// The datagram types a [`Transport`] implementation works in terms of.
///
/// Re-exported rather than left to a direct `udx-udp` dependency so that an out-of-tree
/// `Transport` is guaranteed to be using the same `Transmit` and `RecvMeta` types this
/// crate does.
pub mod udp {
    pub use udx_udp::*;
}
