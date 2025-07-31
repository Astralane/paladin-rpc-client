use crate::constants::{PAL_PORT, PAL_PORT_MEV_PROTECT};
use std::net::{IpAddr, SocketAddr};

#[derive(Debug, Hash, PartialEq, Eq)]
pub struct PalSocketAddr {
    p3_port: SocketAddr,
    revert_protected_port: SocketAddr,
}
impl PalSocketAddr {
    pub fn from_ip(ip: IpAddr) -> Self {
        Self {
            p3_port: SocketAddr::new(ip, PAL_PORT),
            revert_protected_port: SocketAddr::new(ip, PAL_PORT_MEV_PROTECT),
        }
    }
}
