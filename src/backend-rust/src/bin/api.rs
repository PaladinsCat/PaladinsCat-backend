use std::{env, net::SocketAddr, time::Duration};

use paladinscat_backend::{candidate_router, foundation::FoundationState, server::serve_router};
use paladinscat_core::{cache::RedisCache, config::BackendConfig, database::Database};
use tokio::sync::oneshot;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "paladinscat_backend=info".into()),
        )
        .init();

    if env::var("PALADINSCAT_RUST_CANDIDATE_ENABLE").as_deref() != Ok("true") {
        eprintln!(
            "native backend admission is disabled; set PALADINSCAT_RUST_CANDIDATE_ENABLE=true only for local/private candidate validation"
        );
        std::process::exit(78);
    }
    let config = BackendConfig::from_environment().unwrap_or_else(|error| {
        eprintln!("{error}");
        std::process::exit(78);
    });
    let database =
        Database::new(&config, "paladinscat-rust-api-candidate").unwrap_or_else(|error| {
            eprintln!("{error}");
            std::process::exit(78);
        });
    let redis = RedisCache::new(&config.redis_url).unwrap_or_else(|error| {
        eprintln!("invalid REDIS_URL: {error}");
        std::process::exit(78);
    });
    let foundation =
        FoundationState::new(config.clone(), database, redis).unwrap_or_else(|error| {
            eprintln!("{error}");
            std::process::exit(78);
        });
    foundation.initialize().await;
    let address: SocketAddr = format!("{}:{}", config.api_host, config.api_port)
        .parse()
        .unwrap_or_else(|error| {
            eprintln!("invalid native candidate listen address: {error}");
            std::process::exit(78);
        });
    let listener = tokio::net::TcpListener::bind(address)
        .await
        .unwrap_or_else(|error| {
            eprintln!("failed to bind native candidate: {error}");
            std::process::exit(1);
        });
    tracing::info!(%address, "native backend candidate is quiesced");
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
    let server_foundation = foundation.clone();
    let mut server = tokio::spawn(async move {
        serve_router(listener, candidate_router(server_foundation), async {
            let _ = shutdown_rx.await;
        })
        .await
    });

    let mut exit_code = 0;
    tokio::select! {
        result = &mut server => {
            match result {
                Ok(Ok(())) => {}
                Ok(Err(error)) => {
                    eprintln!("native candidate server failed: {error}");
                    exit_code = 1;
                }
                Err(error) => {
                    eprintln!("native candidate server task failed: {error}");
                    exit_code = 1;
                }
            }
        }
        signal = shutdown_signal() => {
            tracing::info!(signal, "shutdown signal received; draining native API");
            let _ = shutdown_tx.send(());
            let timeout = Duration::from_millis(config.shutdown_drain_timeout_ms);
            match tokio::time::timeout(timeout, &mut server).await {
                Ok(Ok(Ok(()))) => {
                    tracing::info!("native API drained cleanly");
                }
                Ok(Ok(Err(error))) => {
                    eprintln!("native candidate server failed during drain: {error}");
                    exit_code = 1;
                }
                Ok(Err(error)) => {
                    eprintln!("native candidate server task failed during drain: {error}");
                    exit_code = 1;
                }
                Err(_) => {
                    tracing::warn!(
                        active_requests = foundation.active_requests.count(),
                        timeout_ms = config.shutdown_drain_timeout_ms,
                        "native API drain timeout expired"
                    );
                }
            }
        }
    }
    foundation.redis.close().await;
    if exit_code != 0 {
        std::process::exit(exit_code);
    }
}

async fn shutdown_signal() -> &'static str {
    #[cfg(unix)]
    {
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                .expect("install SIGTERM handler");
        tokio::select! {
            _ = tokio::signal::ctrl_c() => "SIGINT",
            _ = terminate.recv() => "SIGTERM",
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
        "SIGINT"
    }
}
