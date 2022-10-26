use std::io::{Error, ErrorKind, Result};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::mpsc::Sender;

use http::header::{HeaderMap, HeaderName, HeaderValue};
use http::request::{Parts, Request};
use http::{Method, Version};

use crate::FAST;
use crate::relay::Address as ProxyAddress;
use crate::relay::Request as ProxyRequest;

pub async fn handle(stream: &mut TcpStream, req_tx: Sender<ProxyRequest>) -> Result<()> {
    let mut buf = vec![0u8; 0x2000];
    let n = stream.read(&mut buf).await?;

    let (mut request, head_n) = parse_request(&buf[..n])?;

    // connect https
    if request.method() == Method::CONNECT && request.version() == Version::HTTP_11 {
        let addr = ProxyAddress::DomainAddress(
            request.uri().host().unwrap().into(),
            request.uri().port_u16().unwrap_or(443),
        );
        return handle_connect(stream, req_tx, addr).await;
    }

    // forward http
    rm_proxy_hdrs(request.headers_mut());
    // buf: 0 .. head_m .. head_n .. m .. n
    let (parts, _) = request.into_parts();
    let head_m = write_request_hdr(&parts, &mut buf[..head_n]);
    let m = n - (head_n - head_m);
    unsafe {
        let count = m - head_m;
        let src = buf.as_ptr().add(head_n);
        let dst = buf.as_mut_ptr().add(head_m);
        std::ptr::copy(src, dst, count);
    };
    let addr = ProxyAddress::DomainAddress(
        parts.uri.host().unwrap().into(),
        parts.uri.port_u16().unwrap_or(80),
    );

    log::info!(
        "[http] [{}] [{}] [{}]",
        stream.peer_addr().unwrap(),
        parts.method.as_str(),
        &addr
    );

    let (req, rx) = ProxyRequest::new_connect(addr, unsafe{FAST});
    let _ = req_tx.send(req).await;

    let mut quic_stream = rx.await.map_err(map_io_err)?;
    quic_stream.write_all(&buf[..m]).await?;
	realm_io::bidi_copy(stream, &mut quic_stream).await?;
    tokio::io::copy_bidirectional(stream, &mut quic_stream).await?;

    Ok(())
}

async fn handle_connect(
    stream: &mut TcpStream,
    req_tx: Sender<ProxyRequest>,
    addr: ProxyAddress,
) -> Result<()> {
    log::info!(
        "[http] [{}] [CONNECT] [{}]",
        stream.peer_addr().unwrap(),
        &addr
    );

    let (req, rx) = ProxyRequest::new_connect(addr, unsafe{FAST});
    let _ = req_tx.send(req).await;

    let mut quic_stream = rx.await.map_err(map_io_err)?;
    stream
        .write_all(b"HTTP/1.1 200 Connection Established\r\n\r\n")
        .await?;

    realm_io::bidi_copy(stream, &mut quic_stream).await
}

fn rm_proxy_hdrs(headers: &mut HeaderMap) {
    headers.remove("proxy-connection");
    headers.remove("proxy-authenticate");
    headers.remove("proxy-authorization");
    headers.remove("te");
    headers.remove("trailers");
    headers.remove("transfer-encoding");
    headers.remove("upgrade");

    if let Some(keys) = headers.remove("connection") {
        let keys = std::str::from_utf8(keys.as_bytes()).unwrap();
        for key in keys.split(',').map(str::trim) {
            headers.remove(key);
        }
    }
}

fn write_request_hdr(parts: &Parts, mut buf: &mut [u8]) -> usize {
    use std::io::Write;
    let mut n = 0;

    macro_rules! w {
        ( $b: expr $(, $bx: expr)* ) => {
            n += buf.write($b).unwrap();
            $(
                n += buf.write($bx).unwrap();
            )*
        };
    }

    // first line
    let method = parts.method.as_str().as_bytes();
    let path_query = parts.uri.path_and_query().unwrap().as_str().as_bytes();
    let version = match parts.version {
        Version::HTTP_10 => b"HTTP/1.0",
        Version::HTTP_11 => b"HTTP/1.1",
        _ => unreachable!(),
    };
    w![method, b" ", path_query, b" ", version, b"\r\n"];

    for (key, value) in parts.headers.iter() {
        w!(key.as_str().as_bytes(), b": ", value.as_bytes(), b"\r\n");
    }

    w!(b"\r\n");

    n
}

fn parse_request(buf: &[u8]) -> Result<(Request<()>, usize)> {
    use httparse::{Request, Status, EMPTY_HEADER};

    let mut headers = [EMPTY_HEADER; 128];
    let mut request = Request::new(&mut headers);

    let head_n = match request.parse(buf).map_err(map_io_err)? {
        Status::Complete(n) => n,
        Status::Partial => return Err(new_io_err("unknown protocol")),
    };

    let mut headers = HeaderMap::from_iter(request.headers.iter().map(|hdr| {
        (
            HeaderName::from_bytes(hdr.name.as_bytes()).unwrap(),
            HeaderValue::from_bytes(hdr.value).unwrap(),
        )
    }));

    let version = match request.version.unwrap() {
        0u8 => Version::HTTP_10,
        1u8 => Version::HTTP_11,
        _ => return Err(new_io_err("unsupported http version")),
    };

    let mut builder = http::request::Builder::new()
        .method(request.method.unwrap())
        .uri(request.path.unwrap())
        .version(version);
    std::mem::swap(builder.headers_mut().unwrap(), &mut headers);

    let request = builder.body(()).unwrap();

    Ok((request, head_n))
}

fn map_io_err(e: impl std::error::Error) -> Error {
    new_io_err(&e.to_string())
}

fn new_io_err<T>(reason: T) -> Error
where
    T: Into<String>,
{
    Error::new(ErrorKind::Other, reason.into())
}