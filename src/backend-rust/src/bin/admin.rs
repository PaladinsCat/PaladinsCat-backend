use base64::{Engine as _, engine::general_purpose::STANDARD};
use paladinscat_backend::{
    operators::{
        OperatorServices, migrations_apply, migrations_compare, mitigate_nonranked_raw_json,
        nonranked_raw_json_status, options, pipeline_buffer_status, pipeline_check,
        pipeline_ingest, pipeline_populate, pipeline_process, pipeline_reset_stuck, pipeline_run,
        pipeline_status, private_accounts_backfill, ratings_reingest, recovery_forecast,
        reference_ingest, remove_nonranked_raw_json_guard,
    },
    runtime_status,
};
use serde_json::Value;

#[tokio::main]
async fn main() {
    let arguments = std::env::args().skip(1).collect::<Vec<_>>();
    let result = dispatch(&arguments).await;
    if let Err(error) = result {
        eprintln!("{error:#}");
        std::process::exit(1);
    }
}

async fn dispatch(arguments: &[String]) -> anyhow::Result<()> {
    match arguments.first().map(String::as_str) {
        Some("migration-status") => print_json(runtime_status()),
        Some("deployment-request") => deployment_request(None, None).await,
        Some("deployment-status") => authenticated_get("/admin/deployment/status").await,
        Some("schedulers-ready") => schedulers_assert_running().await,
        Some("pipeline") => {
            let services = OperatorServices::from_environment()?;
            let opts = options(arguments.get(2..).unwrap_or_default());
            let value = match arguments.get(1).map(String::as_str) {
                Some("populate") => pipeline_populate(&services, &opts).await?,
                Some("ingest") | Some("ingest-matches") => {
                    pipeline_ingest(&services, &opts).await?
                }
                Some("process") => pipeline_process(&services, &opts).await?,
                Some("run") => pipeline_run(&services, &opts).await?,
                Some("check") => pipeline_check(&services.database).await?,
                Some("reset-stuck") => pipeline_reset_stuck(&services.database).await?,
                Some("buffer-status") => pipeline_buffer_status(&services.database).await?,
                Some("status") => pipeline_status(&services.database).await?,
                _ => usage_exit(),
            };
            print_json(value);
        }
        Some("reference") if arguments.get(1).map(String::as_str) == Some("ingest") => {
            let kind = arguments
                .get(2)
                .map(String::as_str)
                .unwrap_or_else(|| usage_exit());
            print_json(reference_ingest(&OperatorServices::from_environment()?, kind).await?);
        }
        Some("ratings") if arguments.get(1).map(String::as_str) == Some("reingest") => {
            let services = OperatorServices::from_environment()?;
            print_json(ratings_reingest(&services.database, has_flag(arguments, "--apply")).await?);
        }
        Some("recovery") if arguments.get(1).map(String::as_str) == Some("forecast") => {
            let services = OperatorServices::from_environment()?;
            print_json(recovery_forecast(&services.database).await?);
        }
        Some("storage") if arguments.get(1).map(String::as_str) == Some("raw-json") => {
            let services = OperatorServices::from_environment()?;
            let opts = options(arguments.get(3..).unwrap_or_default());
            match arguments.get(2).map(String::as_str) {
                Some("status") => print_json(nonranked_raw_json_status(&services.database).await?),
                Some("mitigate") if has_flag(arguments, "--apply") => {
                    let batch_size = opts
                        .get("batch-size")
                        .map(|value| value.parse::<usize>())
                        .transpose()?
                        .unwrap_or(5_000);
                    print_json(mitigate_nonranked_raw_json(&services.database, batch_size).await?);
                }
                Some("mitigate") => anyhow::bail!("storage raw-json mitigate requires --apply"),
                Some("remove-guard") if has_flag(arguments, "--apply") => {
                    print_json(remove_nonranked_raw_json_guard(&services.database).await?);
                }
                Some("remove-guard") => {
                    anyhow::bail!("storage raw-json remove-guard requires --apply")
                }
                _ => usage_exit(),
            }
        }
        Some("migrations") if arguments.get(1).map(String::as_str) == Some("compare") => {
            let opts = options(arguments.get(2..).unwrap_or_default());
            let local = opts
                .get("local-url")
                .cloned()
                .or_else(|| std::env::var("LOCAL_DATABASE_URL").ok())
                .unwrap_or_else(|| required_env("LOCAL_DATABASE_URL"));
            let remote = opts
                .get("remote-url")
                .cloned()
                .or_else(|| std::env::var("REMOTE_DATABASE_URL").ok())
                .unwrap_or_else(|| required_env("REMOTE_DATABASE_URL"));
            let report = migrations_compare(&local, &remote).await?;
            let matches = report["matches"].as_bool() == Some(true);
            print_json(report);
            if !matches {
                std::process::exit(2);
            }
        }
        Some("migrations") if arguments.get(1).map(String::as_str) == Some("apply") => {
            let services = OperatorServices::from_environment()?;
            print_json(migrations_apply(&services.database).await?);
        }
        Some("private-accounts") if arguments.get(1).map(String::as_str) == Some("backfill") => {
            let services = OperatorServices::from_environment()?;
            print_json(
                private_accounts_backfill(&services.database, has_flag(arguments, "--apply"))
                    .await?,
            );
        }
        Some("deployment") if arguments.get(1).map(String::as_str) == Some("set") => {
            let phase = arguments.get(2).cloned();
            let opts = options(arguments.get(3..).unwrap_or_default());
            deployment_request(phase, opts.get("payload-b64").cloned()).await;
        }
        Some("deployment") if arguments.get(1).map(String::as_str) == Some("assert-quiesced") => {
            deployment_assert_quiesced().await;
        }
        Some("schedulers") if arguments.get(1).map(String::as_str) == Some("assert-running") => {
            schedulers_assert_running().await;
        }
        _ => usage_exit(),
    }
    Ok(())
}

fn print_json(value: impl serde::Serialize) {
    match serde_json::to_string_pretty(&value) {
        Ok(s) => println!("{}", s),
        Err(error) => {
            eprintln!("Failed to serialize JSON: {error}");
            std::process::exit(1);
        }
    }
}

async fn deployment_request(phase: Option<String>, payload_b64: Option<String>) {
    let endpoint = phase.unwrap_or_else(|| required_env("PC_DEPLOY_ENDPOINT"));
    if !matches!(endpoint.as_str(), "state" | "drain" | "warm") {
        eprintln!("invalid deployment endpoint");
        std::process::exit(64);
    }
    let encoded = payload_b64.unwrap_or_else(|| required_env("PC_DEPLOY_PAYLOAD_B64"));
    let payload = STANDARD.decode(encoded).unwrap_or_else(|error| {
        eprintln!("invalid deployment payload: {error}");
        std::process::exit(64);
    });
    let response = client()
        .post(format!("{}/admin/deployment/{endpoint}", api_origin()))
        .bearer_auth(required_env("ADMIN_SECRET"))
        .header("content-type", "application/json")
        .body(payload)
        .send()
        .await
        .unwrap_or_else(|error| transport_exit(error));
    print_response_or_exit(response).await;
}

async fn authenticated_get(path: &str) {
    let response = client()
        .get(format!("{}{path}", api_origin()))
        .bearer_auth(required_env("ADMIN_SECRET"))
        .send()
        .await
        .unwrap_or_else(|error| transport_exit(error));
    print_response_or_exit(response).await;
}

async fn deployment_assert_quiesced() {
    let response = client()
        .get(format!("{}/admin/deployment/status", api_origin()))
        .bearer_auth(required_env("ADMIN_SECRET"))
        .send()
        .await
        .unwrap_or_else(|error| transport_exit(error));
    let status = response.status();
    let body = response.text().await.unwrap_or_default();
    if !status.is_success() {
        eprintln!("{body}");
        std::process::exit(1);
    }
    let value: Value = serde_json::from_str(&body).unwrap_or_else(|error| {
        eprintln!("invalid deployment response: {error}");
        std::process::exit(1);
    });
    let state = value
        .get("state")
        .and_then(Value::as_str)
        .or_else(|| value.pointer("/deployment/state").and_then(Value::as_str));
    if !matches!(state, Some("quiesced" | "drained")) {
        eprintln!("{body}");
        std::process::exit(1);
    }
    println!("{body}");
}

async fn schedulers_assert_running() {
    let response = client()
        .get(format!("{}/schedulers", api_origin()))
        .send()
        .await
        .unwrap_or_else(|error| transport_exit(error));
    let status = response.status();
    let body = response.text().await.unwrap_or_default();
    if !status.is_success() {
        eprintln!("{body}");
        std::process::exit(1);
    }
    let value: Value = serde_json::from_str(&body).unwrap_or_else(|error| {
        eprintln!("invalid scheduler response: {error}");
        std::process::exit(1);
    });
    let ready = value.as_object().is_some_and(|rows| {
        !rows.is_empty()
            && rows
                .values()
                .all(|row| row.get("enabled").and_then(Value::as_bool) == Some(true))
    });
    if !ready {
        eprintln!("{body}");
        std::process::exit(1);
    }
    println!("{body}");
}

async fn print_response_or_exit(response: reqwest::Response) {
    let status = response.status();
    let body = response.text().await.unwrap_or_default();
    if !body.is_empty() {
        println!("{body}");
    }
    if !status.is_success() {
        std::process::exit(1);
    }
}
fn client() -> reqwest::Client {
    reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(300))
        .build()
        .unwrap_or_else(|error| {
            eprintln!("failed to construct admin client: {error}");
            std::process::exit(1);
        })
}
fn api_origin() -> String {
    std::env::var("PALADINSCAT_ADMIN_API_ORIGIN")
        .unwrap_or_else(|_| "http://127.0.0.1:3005".to_owned())
        .trim_end_matches('/')
        .to_owned()
}
fn required_env(name: &str) -> String {
    std::env::var(name)
        .ok()
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| {
            eprintln!("{name} is required");
            std::process::exit(78);
        })
}
fn transport_exit(error: reqwest::Error) -> ! {
    eprintln!("{error}");
    std::process::exit(1);
}
fn has_flag(arguments: &[String], flag: &str) -> bool {
    arguments.iter().any(|argument| argument == flag)
}
fn usage_exit() -> ! {
    eprintln!(
        "usage: paladinscat-admin <pipeline|reference|ratings|recovery|storage|migrations|private-accounts|deployment|schedulers> <command>"
    );
    std::process::exit(64);
}
