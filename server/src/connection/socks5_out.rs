use std::io::{Error, Result, ErrorKind};
use std::net::SocketAddr;
use tokio::net::TcpStream;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tuic_protocol::Address;

mod config {
    use super::*;
    use std::sync::Once;
    use std::mem::MaybeUninit;

    static INIT: Once = Once::new();
    static mut DEST: MaybeUninit<SocketAddr> =  MaybeUninit::uninit();

    pub fn set_server(addr: SocketAddr) {
        INIT.call_once(||{
            unsafe { DEST.write(addr) };
        });
    }

    pub fn get_server() -> SocketAddr {
        unsafe { DEST.as_ptr().read() }
    }

    pub fn is_inited() -> bool {
        INIT.is_completed()
    }
}

mod proto {
    use super::*;
    use bytes::BufMut;

    pub const NOAUTH_REQ: &[u8] = &[0x05, 0x01, 0x00];
    pub const NOAUTH_RES: &[u8] = &[0x05, 0x00];
    pub const CONNECT_REQ: &[u8] = &[0x05, 0x01, 0x00];
    pub const CONNECT_RES: &[u8] = &[0x05, 0x00, 0x00];

    pub const ADDR_IPV4: u8 = 0x01;
    pub const ADDR_IPV6: u8 = 0x04;
    pub const ADDR_FQDN: u8 = 0x03;

    fn cvt(addr: &Address) -> u8 {
        match addr {
            Address::DomainAddress(..) => ADDR_FQDN,
            Address::SocketAddress(SocketAddr::V4(..)) => ADDR_IPV4,
            Address::SocketAddress(SocketAddr::V6(..)) => ADDR_IPV6,
        }
    }

    pub fn write_request(addr: &Address, mut buf: &mut [u8]) -> usize {
        let total = buf.len();
        buf.put_slice(CONNECT_REQ);

        let ptr = buf.as_mut_ptr();
        addr.write_to_buf(&mut buf);
        unsafe{ *ptr = cvt(addr) };
        total - buf.len()
    }

    pub fn check_noauth(buf: &[u8]) -> Result<()> {
        if buf != NOAUTH_RES {
            Err(Error::new(ErrorKind::Other, "socks5 auth error"))
        } else {
            Ok(())
        }
    }

    pub fn check_response(buf: &[u8]) -> Result<()> {
        if buf.len() < 3 + 1 + 4 + 2 || &buf[..3] != CONNECT_RES {
            Err(Error::new(ErrorKind::Other, "socks5 connect error"))
        } else {
            Ok(())
        }
    }
}

pub use config::*;
pub async fn connect(addr: Address) -> Result<TcpStream> {
    let mut buf = [0u8; 512];
    let mut stream = TcpStream::connect(config::get_server()).await?;
    log::debug!("[connect-socks5] start handshake");
    // --->
    stream.write_all(proto::NOAUTH_REQ).await?;

    // <---
    stream.read_exact(&mut buf[..2]).await?;
    proto::check_noauth(&buf[..2])?;

    // --->
    let n = proto::write_request(&addr, &mut buf);
    stream.write_all(&buf[..n]).await?;

    // <---
    let n = stream.read(&mut buf).await?;
    proto::check_response(&buf[..n])?;

    log::debug!("[connect-socks5] connection established");
    Ok(stream)
}