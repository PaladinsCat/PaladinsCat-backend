<div align="center">
  <h1>PaladinsCat Backend</h1>
  <p>Rust services, workers, and data contracts behind <a href="https://paladinscat.com/">PaladinsCat</a>.</p>

  [![CI](https://github.com/PaladinsCat/PaladinsCat-backend/actions/workflows/ci.yml/badge.svg)](https://github.com/PaladinsCat/PaladinsCat-backend/actions/workflows/ci.yml)
  [![CodeQL](https://github.com/PaladinsCat/PaladinsCat-backend/actions/workflows/codeql.yml/badge.svg)](https://github.com/PaladinsCat/PaladinsCat-backend/actions/workflows/codeql.yml)
</div>

This workspace contains the API, ingestion and recovery workers, Hi-Rez relay, shared data contracts, database migrations, operational tooling, and the evidence-preserving pipelines used by PaladinsCat.

## Workspace

| Crate | Purpose |
| --- | --- |
| `paladinscat-backend` | Axum API, background workers, administration tools, and database integration |
| `paladinscat-hirez-relay` | Upstream request dispatch, normalization, recovery, and provider controls |
| `paladinscat-core` | Shared configuration, caching, search, region, and deployment contracts |
| `migrations/` | Ordered PostgreSQL schema history |

## Development

```text
cargo build --workspace
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
```

Run `cargo fmt --all --check` before submitting a pull request. All commands above
plus the format check are the mandatory local validation gate. See [CONTRIBUTING.md](CONTRIBUTING.md) for the contribution workflow and [SECURITY.md](SECURITY.md) for private vulnerability reporting.

Licensed under the [MIT License](LICENSE).
