use crate::connection::Connection;
use futures_util::StreamExt;
use quinn::{Endpoint, EndpointConfig, Incoming, ServerConfig, VarInt};
use socket2::{Domain, Protocol, SockAddr, Socket, Type};
use std::{
    collections::HashSet,
    io::Result,
    net::{SocketAddr, UdpSocket},
    sync::Arc,
    time::Duration,
};

pub struct Server {
    incoming: Incoming,
    listen_addr: SocketAddr,
    token: Arc<HashSet<[u8; 32]>>,
    authentication_timeout: Duration,
    max_pkt_size: usize,
    max_concurrent_stream: VarInt,
}

impl Server {
    pub fn init(
        config: ServerConfig,
        listen_addr: SocketAddr,
        token: HashSet<[u8; 32]>,
        auth_timeout: Duration,
        max_pkt_size: usize,
        max_concurrent_stream: VarInt,
    ) -> Result<Self> {
        let socket = match listen_addr {
            SocketAddr::V4(_) => UdpSocket::bind(listen_addr)?,
            SocketAddr::V6(_) => {
                let socket = Socket::new(Domain::IPV6, Type::DGRAM, Some(Protocol::UDP))?;
                socket.set_only_v6(false)?;
                socket.bind(&SockAddr::from(listen_addr))?;
                UdpSocket::from(socket)
            }
        };

        let (_, incoming) = Endpoint::new(EndpointConfig::default(), Some(config), socket)?;

        Ok(Self {
            incoming,
            listen_addr,
            token: Arc::new(token),
            authentication_timeout: auth_timeout,
            max_pkt_size,
            max_concurrent_stream,
        })
    }

    pub async fn run(mut self) {
        log::info!("Server started. Listening: {}", self.listen_addr);

        while let Some(conn) = self.incoming.next().await {
            tokio::spawn(Connection::handle(
                conn,
                self.token.clone(),
                self.authentication_timeout,
                self.max_pkt_size,
                self.max_concurrent_stream,
            ));
        }
    }
}
