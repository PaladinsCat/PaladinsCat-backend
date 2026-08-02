use paladinscat_backend::workers::{
    coordination::SCHEDULER_KEYS, match_lifecycle::MatchLifecycleRepository,
    scheduler_host::run_scheduler_domain, scheduler_runtime::SchedulerRuntimeExit,
};
use paladinscat_core::{config::BackendConfig, database::Database};

const MAX_ADOPTION_PAGE: usize = 10_000;

#[tokio::main]
async fn main() {
    let mut arguments = std::env::args().skip(1);
    match arguments.next().as_deref() {
        Some("adopt-nonranked") => run_adoption(arguments).await,
        Some("run-scheduler") => run_scheduler(arguments).await,
        Some("run-all-schedulers") => run_all_schedulers(arguments).await,
        _ => usage_exit(),
    }
}

async fn run_adoption(mut arguments: impl Iterator<Item = String>) {
    let mut limit = 1_000_usize;
    let mut apply = false;
    while let Some(argument) = arguments.next() {
        if argument == "--apply" {
            apply = true;
            continue;
        }
        if argument != "--limit" {
            eprintln!("unknown argument: {argument}");
            std::process::exit(64);
        }
        let Some(value) = arguments.next() else {
            eprintln!("--limit requires a positive integer");
            std::process::exit(64);
        };
        limit = value.parse::<usize>().unwrap_or_else(|_| {
            eprintln!("invalid --limit: {value}");
            std::process::exit(64);
        });
    }
    if limit == 0 || limit > MAX_ADOPTION_PAGE {
        eprintln!("--limit must be between 1 and {MAX_ADOPTION_PAGE}");
        std::process::exit(64);
    }
    if apply && !environment_enabled("PALADINSCAT_RUST_WORKER_MAINTENANCE_ENABLE") {
        eprintln!(
            "DB-only maintenance writes are disabled; set \
             PALADINSCAT_RUST_WORKER_MAINTENANCE_ENABLE=true explicitly"
        );
        std::process::exit(78);
    }

    let database = worker_database("paladinscat-rust-nonranked-adoption");
    let repository = MatchLifecycleRepository::new(database);
    let summary = if apply {
        repository.adopt_nonranked_batch(limit).await
    } else {
        repository.preview_nonranked_adoption(limit).await
    }
    .unwrap_or_else(|error| {
        eprintln!("non-ranked adoption failed: {error}");
        std::process::exit(1);
    });
    println!(
        "{}",
        serde_json::to_string_pretty(&serde_json::json!({
            "mode": if apply { "apply" } else { "preview" },
            "limit": limit,
            "summary": summary,
        }))
        .expect("serialize adoption summary")
    );
}

async fn run_scheduler(mut arguments: impl Iterator<Item = String>) {
    let Some(scheduler_key) = arguments.next() else {
        usage_exit();
    };
    if arguments.next().is_some() || !SCHEDULER_KEYS.contains(&scheduler_key.as_str()) {
        eprintln!("unknown scheduler {scheduler_key}");
        std::process::exit(64);
    }
    if !environment_enabled("PALADINSCAT_RUST_WORKER_ENABLE") {
        eprintln!(
            "native scheduler execution is disabled; set \
             PALADINSCAT_RUST_WORKER_ENABLE=true explicitly"
        );
        std::process::exit(78);
    }
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();
    let (config, database) = worker_context(&format!("paladinscat-rust-{scheduler_key}"));
    let owner_id = format!(
        "rust:{}:{}:{}",
        std::env::var("HOSTNAME")
            .or_else(|_| std::env::var("COMPUTERNAME"))
            .unwrap_or_else(|_| "worker".to_owned()),
        std::process::id(),
        uuid::Uuid::new_v4(),
    );
    let exit = run_scheduler_domain(
        database,
        std::sync::Arc::new(config),
        scheduler_key.clone(),
        owner_id,
    )
    .await
    .unwrap_or_else(|error| {
        eprintln!("{scheduler_key} scheduler failed: {error}");
        std::process::exit(1);
    });
    match exit {
        SchedulerRuntimeExit::Shutdown => {}
        SchedulerRuntimeExit::OwnershipUnavailable => {
            eprintln!(
                "{scheduler_key} is not assigned to Rust or another owner holds its live lease"
            );
            std::process::exit(75);
        }
        SchedulerRuntimeExit::OwnershipLost => {
            eprintln!("tier_stats ownership was lost; scheduler stopped after draining");
            std::process::exit(1);
        }
    }
}

async fn run_all_schedulers(mut arguments: impl Iterator<Item = String>) {
    if arguments.next().is_some() {
        usage_exit();
    }
    if !environment_enabled("PALADINSCAT_RUST_WORKER_ENABLE") {
        eprintln!(
            "native scheduler execution is disabled; set \
             PALADINSCAT_RUST_WORKER_ENABLE=true explicitly"
        );
        std::process::exit(78);
    }
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();
    let (config, database) = worker_context("paladinscat-rust-scheduler-host");
    let config = std::sync::Arc::new(config);
    let mut tasks = tokio::task::JoinSet::new();
    for scheduler_key in SCHEDULER_KEYS {
        let scheduler_key = (*scheduler_key).to_owned();
        let config = config.clone();
        let database = database.clone();
        let owner_id = format!(
            "rust:{}:{}:{}:{}",
            std::env::var("HOSTNAME")
                .or_else(|_| std::env::var("COMPUTERNAME"))
                .unwrap_or_else(|_| "worker".to_owned()),
            std::process::id(),
            scheduler_key,
            uuid::Uuid::new_v4()
        );
        tasks.spawn(async move {
            let result = loop {
                let result = run_scheduler_domain(
                    database.clone(),
                    config.clone(),
                    scheduler_key.clone(),
                    owner_id.clone(),
                )
                .await;
                if matches!(
                    result.as_ref(),
                    Ok(SchedulerRuntimeExit::OwnershipUnavailable)
                ) {
                    tracing::warn!(%scheduler_key, "scheduler ownership unavailable; retrying");
                    tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                    continue;
                }
                break result;
            };
            (scheduler_key, result)
        });
    }
    while let Some(result) = tasks.join_next().await {
        match result {
            Ok((scheduler, Ok(SchedulerRuntimeExit::Shutdown))) => {
                tracing::info!(%scheduler, "scheduler stopped");
            }
            Ok((scheduler, Ok(exit))) => {
                eprintln!("{scheduler} scheduler stopped unexpectedly: {exit:?}");
                tasks.abort_all();
                std::process::exit(1);
            }
            Ok((scheduler, Err(error))) => {
                eprintln!("{scheduler} scheduler failed: {error}");
                tasks.abort_all();
                std::process::exit(1);
            }
            Err(error) => {
                eprintln!("scheduler task failed: {error}");
                tasks.abort_all();
                std::process::exit(1);
            }
        }
    }
}

fn worker_database(application_name: &str) -> Database {
    worker_context(application_name).1
}

fn worker_context(application_name: &str) -> (BackendConfig, Database) {
    let config = BackendConfig::from_environment().unwrap_or_else(|error| {
        eprintln!("{error}");
        std::process::exit(78);
    });
    let database = Database::new(&config, application_name).unwrap_or_else(|error| {
        eprintln!("{error}");
        std::process::exit(78);
    });
    (config, database)
}

fn environment_enabled(name: &str) -> bool {
    std::env::var(name).as_deref() == Ok("true")
}

fn usage_exit() -> ! {
    eprintln!(
        "usage:\n  paladinscat-worker adopt-nonranked [--limit N] [--apply]\n  \
         paladinscat-worker run-scheduler <ranked_tracker|auto_ingester|baseline_tracker|derived_projection_tracker|hourly_gap_checker|tier_stats>\n  \
         paladinscat-worker run-all-schedulers"
    );
    std::process::exit(64);
}
