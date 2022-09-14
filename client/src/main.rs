use crate::config::{Config, ConfigError};
use opentelemetry::runtime::Tokio;
use std::{env, error::Error, process};
use tracing_log::env_logger::BuilderExt;
use tracing_subscriber::{prelude::*, Registry};

mod certificate;
mod config;
mod relay;
mod socks5;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args = env::args_os();

    let config = Config::parse(args).map_err(|err| {
        match &err {
            ConfigError::Help(help) => println!("{help}"),
            ConfigError::Version(version) => println!("{version}"),
            err => eprintln!("{err}"),
        };
        err
    })?;

    env_logger::builder()
        .filter_level(config.log_level)
        .format_level(true)
        .format_module_path(false)
        .emit_traces()
        .init();

    let tracer = opentelemetry_jaeger::new_pipeline()
        .with_auto_split_batch(true)
        .install_batch(Tokio)?;
    let telemetry = tracing_opentelemetry::layer().with_tracer(tracer);
    let subscriber = Registry::default().with(telemetry);

    tracing::subscriber::set_global_default(subscriber)?;

    let (relay, req_tx) = relay::init(
        config.client_config,
        config.server_addr,
        config.token_digest,
        config.heartbeat_interval,
        config.reduce_rtt,
        config.udp_relay_mode,
        config.request_timeout,
        config.max_udp_relay_packet_size,
    )
    .await;

    let socks5 = socks5::init(config.local_addr, config.socks5_auth, req_tx).await?;

    tokio::select! {
        res = relay => res,
        res = socks5 => res,
    };

    process::exit(1);
}
