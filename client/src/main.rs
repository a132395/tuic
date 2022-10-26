use crate::config::{Config, ConfigError};
use std::{env, process};

mod certificate;
mod config;
mod relay;
mod http;
mod socks5;

#[cfg(unix)]
#[global_allocator]
static GLOBAL: jemallocator::Jemalloc = jemallocator::Jemalloc;

static mut FAST: bool = false;

fn main() {
    let args = env::args_os();

    let config = match Config::parse(args) {
        Ok(cfg) => cfg,
        Err(err) => {
            match err {
                ConfigError::Help(help) => println!("{help}"),
                ConfigError::Version(version) => println!("{version}"),
                err => eprintln!("{err}"),
            }
            return;
        }
    };

    env_logger::builder()
        .filter_level(config.log_level)
        .format_level(true)
        .format_target(false)
        .format_module_path(false)
        .init();

    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(run(config))
}

async fn run(config: Config) {
    let (relay, req_tx) = relay::init(
        config.client_config,
        config.server_addr,
        config.token_digest,
        config.heartbeat_interval,
        config.reduce_rtt,
        config.udp_relay_mode,
        config.request_timeout,
        config.max_udp_relay_packet_size,
		config.max_concurrent_stream,
    )
    .await;

    let socks5 = match socks5::init(config.local_addr, config.socks5_auth, req_tx).await {
        Ok(socks5) => socks5,
        Err(err) => {
            eprintln!("{err}");
            return;
        }
    };

    tokio::select! {
        res = relay => res,
        res = socks5 => res,
    };

    process::exit(1);
}
