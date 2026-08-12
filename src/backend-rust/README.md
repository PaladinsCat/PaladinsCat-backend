# PaladinsCat Rust backend

This Cargo workspace contains the production API, worker, administrative
command, shared core, and Hi-Rez relay crates. Runtime selection and deployment
belong to `paladinscat-platform`; database schema authority belongs to this
repository's `migrations/tracked/` directory.

The principal backend binaries are:

- `paladinscat-api`
- `paladinscat-worker`
- `paladinscat-admin`

Production enables the API explicitly with
`PALADINSCAT_RUST_PRODUCTION_ENABLE=true`. Candidate mode remains available for
bounded compatibility testing, but must not be enabled at the same time.

Workspace validation:

```powershell
cargo fmt --all --check
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
```

Use the platform Compose wrapper for integration tests. Do not restore retired
monorepo `scripts/migration/*` commands; their evidence is retained only in the
internal wiki archive.
