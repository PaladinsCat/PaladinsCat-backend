use paladinscat_backend::candidate_status;

fn main() {
    let command = std::env::args().nth(1);
    if command.as_deref() != Some("migration-status") {
        eprintln!("usage: paladinscat-admin migration-status");
        std::process::exit(64);
    }
    println!(
        "{}",
        serde_json::to_string_pretty(&candidate_status()).expect("serialize candidate status")
    );
}
