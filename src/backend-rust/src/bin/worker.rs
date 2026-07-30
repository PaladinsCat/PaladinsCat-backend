use paladinscat_backend::workers::match_lifecycle::MatchLifecycleRepository;
use paladinscat_core::{config::BackendConfig, database::Database};

const MAX_ADOPTION_PAGE: usize = 10_000;

#[tokio::main]
async fn main() {
    let mut arguments = std::env::args().skip(1);
    let command = arguments.next();
    if command.as_deref() != Some("adopt-nonranked") {
        eprintln!(
            "native scheduler ownership is disabled; usage: \
             paladinscat-worker adopt-nonranked [--limit N] [--apply]"
        );
        std::process::exit(78);
    }

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
    if apply && std::env::var("PALADINSCAT_RUST_WORKER_MAINTENANCE_ENABLE").as_deref() != Ok("true")
    {
        eprintln!(
            "DB-only maintenance writes are disabled; set \
             PALADINSCAT_RUST_WORKER_MAINTENANCE_ENABLE=true explicitly"
        );
        std::process::exit(78);
    }

    let config = BackendConfig::from_environment().unwrap_or_else(|error| {
        eprintln!("{error}");
        std::process::exit(78);
    });
    let database =
        Database::new(&config, "paladinscat-rust-nonranked-adoption").unwrap_or_else(|error| {
            eprintln!("{error}");
            std::process::exit(78);
        });
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
