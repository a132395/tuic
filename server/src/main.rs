use crate::{
    config::{Config, ConfigError},
    server::Server,
};
use std::{env, process};

mod certificate;
mod config;
mod connection;
mod server;

#[cfg(unix)]
#[global_allocator]
static GLOBAL: jemallocator::Jemalloc = jemallocator::Jemalloc;

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

    let server = match Server::init(
        config.server_config,
        config.listen_addr,
        config.token,
        config.authentication_timeout,
        config.max_udp_relay_packet_size,
		config.max_concurrent_stream,
    ) {
        Ok(server) => server,
        Err(err) => {
            eprintln!("{err}");
            return;
        }
    };

    server.run().await;
    process::exit(1);
}
