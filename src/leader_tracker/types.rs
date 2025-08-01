use crate::constants::{PAL_PORT, PAL_PORT_MEV_PROTECT};
use std::net::{IpAddr, SocketAddr};

// [p3 port, revert_protected_port]
pub type PaladinSocketAddrs = [SocketAddr; 2];
pub fn pal_socks_from_ip(ip: IpAddr) -> PaladinSocketAddrs {
    [
        SocketAddr::new(ip, PAL_PORT),
        SocketAddr::new(ip, PAL_PORT_MEV_PROTECT),
    ]
}
