use paladinscat_backend::workers::{
    coordination::WorkerCoordinationRepository,
    match_lifecycle::MatchLifecycleRepository,
    scheduler_runtime::{SchedulerRuntimeExit, run_tier_stats_scheduler},
    tier_stats::TierStatsRepository,
};
use paladinscat_core::{config::BackendConfig, database::Database};

const MAX_ADOPTION_PAGE: usize = 10_000;

#[tokio::main]
async fn main() {
    let mut arguments = std::env::args().skip(1);
    match arguments.next().as_deref() {
        Some("adopt-nonranked") => run_adoption(arguments).await,
        Some("run-scheduler") => run_scheduler(arguments).await,
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
    if arguments.next().is_some() || scheduler_key != "tier_stats" {
        eprintln!(
            "scheduler {scheduler_key} is not an implementation-verified Rust candidate; \
             only tier_stats is currently runnable"
        );
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
    let database = worker_database("paladinscat-rust-tier-stats");
    let owner_id = format!(
        "rust:{}:{}:{}",
        std::env::var("HOSTNAME")
            .or_else(|_| std::env::var("COMPUTERNAME"))
            .unwrap_or_else(|_| "worker".to_owned()),
        std::process::id(),
        uuid::Uuid::new_v4(),
    );
    let exit = run_tier_stats_scheduler(
        WorkerCoordinationRepository::new(database.clone()),
        TierStatsRepository::new(database),
        owner_id,
    )
    .await
    .unwrap_or_else(|error| {
        eprintln!("tier_stats scheduler failed: {error}");
        std::process::exit(1);
    });
    match exit {
        SchedulerRuntimeExit::Shutdown => {}
        SchedulerRuntimeExit::OwnershipUnavailable => {
            eprintln!("tier_stats is not assigned to Rust or another owner holds its live lease");
            std::process::exit(75);
        }
        SchedulerRuntimeExit::OwnershipLost => {
            eprintln!("tier_stats ownership was lost; scheduler stopped after draining");
            std::process::exit(1);
        }
    }
}

fn worker_database(application_name: &str) -> Database {
    let config = BackendConfig::from_environment().unwrap_or_else(|error| {
        eprintln!("{error}");
        std::process::exit(78);
    });
    Database::new(&config, application_name).unwrap_or_else(|error| {
        eprintln!("{error}");
        std::process::exit(78);
    })
}

fn environment_enabled(name: &str) -> bool {
    std::env::var(name).as_deref() == Ok("true")
}

fn usage_exit() -> ! {
    eprintln!(
        "usage:\n  paladinscat-worker adopt-nonranked [--limit N] [--apply]\n  \
         paladinscat-worker run-scheduler tier_stats"
    );
    std::process::exit(64);
}
