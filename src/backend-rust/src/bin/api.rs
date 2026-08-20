use std::{env, net::SocketAddr, sync::Arc, time::Duration};

use paladinscat_backend::{
    candidate_router,
    foundation::FoundationState,
    production_runtime_enabled,
    server::serve_router,
    workers::{
        cache_warmer::CacheWarmer, coordination::SCHEDULER_KEYS,
        scheduler_host::run_scheduler_domain,
    },
};
use paladinscat_core::{cache::RedisCache, config::BackendConfig, database::Database};
use tokio::sync::oneshot;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "paladinscat_backend=info,paladinscat_core=error".into()),
        )
        .init();

    let candidate_enabled = env::var("PALADINSCAT_RUST_CANDIDATE_ENABLE").as_deref() == Ok("true");
    let production_enabled = production_runtime_enabled();
    if candidate_enabled == production_enabled {
        eprintln!(
            "select exactly one native backend mode: PALADINSCAT_RUST_CANDIDATE_ENABLE=true or PALADINSCAT_RUST_PRODUCTION_ENABLE=true"
        );
        std::process::exit(78);
    }
    let config = BackendConfig::from_environment().unwrap_or_else(|error| {
        eprintln!("{error}");
        std::process::exit(78);
    });
    let database = Database::new(
        &config,
        if production_enabled {
            "paladinscat-rust-api"
        } else {
            "paladinscat-rust-api-candidate"
        },
    )
    .unwrap_or_else(|error| {
        eprintln!("{error}");
        std::process::exit(78);
    });
    let redis = RedisCache::new(&config.redis_url).unwrap_or_else(|error| {
        eprintln!("invalid REDIS_URL: {error}");
        std::process::exit(78);
    });
    let runtime_database = database.clone();
    let cache_warmer = CacheWarmer::new(database.clone(), &config).unwrap_or_else(|error| {
        eprintln!("{error}");
        std::process::exit(78);
    });
    let foundation =
        FoundationState::new(config.clone(), database, redis).unwrap_or_else(|error| {
            eprintln!("{error}");
            std::process::exit(78);
        });
    foundation.initialize().await;
    eprintln!("[init] foundation.initialize done");
    let address: SocketAddr = format!("{}:{}", config.api_host, config.api_port)
        .parse()
        .unwrap_or_else(|error| {
            eprintln!("invalid native candidate listen address: {error}");
            std::process::exit(78);
        });
    eprintln!("[init] address parsed: {}", address);
    let listener = tokio::net::TcpListener::bind(address)
        .await
        .unwrap_or_else(|error| {
            eprintln!("failed to bind native candidate: {error}");
            std::process::exit(1);
        });
    eprintln!("[init] listener bound on {}", address);
    tracing::info!(%address, mode=if production_enabled { "production" } else { "candidate" }, "native backend listening");
    let scheduler_tasks = if env_enabled("BACKEND_SCHEDULERS_ENABLED", false) {
        let config = Arc::new(config.clone());
        SCHEDULER_KEYS
            .iter()
            .map(|scheduler_key| {
                let database = runtime_database.clone();
                let config = config.clone();
                let scheduler_key = (*scheduler_key).to_owned();
                let owner_id = format!(
                    "rust:{}:{}:{}",
                    env::var("HOSTNAME")
                        .or_else(|_| env::var("COMPUTERNAME"))
                        .unwrap_or_else(|_| "backend".to_owned()),
                    std::process::id(),
                    uuid::Uuid::new_v4()
                );
                tokio::spawn(async move {
                    if let Err(error) =
                        run_scheduler_domain(database, config, scheduler_key.clone(), owner_id).await
                    {
                        tracing::error!(scheduler=%scheduler_key,%error,"embedded scheduler stopped");
                    }
                })
            })
            .collect::<Vec<_>>()
    } else {
        Vec::new()
    };
    let main_warmer = env_enabled("SITE_CACHE_WARMER_ENABLED", true).then(|| {
        let warmer = cache_warmer.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(env_u64(
                "SITE_CACHE_WARM_STARTUP_DELAY_MS",
                5_000,
            )))
            .await;
            let mut interval = tokio::time::interval(Duration::from_millis(env_u64(
                "SITE_CACHE_WARM_INTERVAL_MS",
                600_000,
            )));
            interval.tick().await;
            loop {
                match warmer.warm_main_site().await {
                    Ok((api, pages)) => tracing::info!(
                        api_warmed = api.warmed,
                        api_deferred = api.deferred,
                        api_failed = api.failed,
                        pages_warmed = pages.warmed,
                        pages_failed = pages.failed,
                        "site cache warm cycle complete"
                    ),
                    Err(error) => tracing::warn!(error = %error, "site cache warm cycle failed"),
                }
                interval.tick().await;
            }
        })
    });
    let champion_warmer = env_enabled("CHAMPION_PAGE_CACHE_WARMER_ENABLED", true).then(|| {
        let warmer = cache_warmer;
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(env_u64(
                "CHAMPION_PAGE_CACHE_WARM_STARTUP_DELAY_MS",
                60_000,
            )))
            .await;
            let mut interval = tokio::time::interval(Duration::from_millis(env_u64(
                "CHAMPION_PAGE_CACHE_WARM_INTERVAL_MS",
                600_000,
            )));
            loop {
                let _ = warmer.warm_champion_pages().await;
                interval.tick().await;
            }
        })
    });
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
    if let Some(task) = main_warmer {
        task.abort();
    }
    if let Some(task) = champion_warmer {
        task.abort();
    }
    for task in scheduler_tasks {
        task.abort();
    }
    foundation.redis.close().await;
    if exit_code != 0 {
        std::process::exit(exit_code);
    }
}

fn env_u64(name: &str, fallback: u64) -> u64 {
    env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .filter(|value| *value > 0)
        .unwrap_or(fallback)
}

fn env_enabled(name: &str, fallback: bool) -> bool {
    env::var(name)
        .ok()
        .map(|value| value.eq_ignore_ascii_case("true") || value == "1")
        .unwrap_or(fallback)
}

async fn shutdown_signal() -> &'static str {
    #[cfg(unix)]
    {
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                .unwrap_or_else(|error| {
                    eprintln!("Failed to install SIGTERM handler: {error}");
                    std::process::exit(78);
                });
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
