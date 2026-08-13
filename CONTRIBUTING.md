# Contributing to PaladinsCat Backend

## Development Setup
1. Install Rust (stable): `rustup default stable`
2. Build: `cargo build`
3. Lint: `cargo clippy -- -D warnings`
4. Test: `cargo test`
5. Format: `cargo fmt`

## Branch Naming
- Features: `feat/description`
- Fixes: `fix/description`
- Refactors: `refactor/description`

## Pull Requests
- Reference an issue number in the PR title
- Include a brief description of changes
- Ensure CI passes (cargo check, clippy, test, fmt)

## Code Style
- Rust: `rustfmt` with 4-space indentation
- Modules: organize by feature in `src/backend-rust/src/`
