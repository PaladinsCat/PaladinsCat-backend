# Contributing to PaladinsCat Backend

## Development Setup
1. Install Rust (stable): `rustup default stable`
2. Build: `cargo build`
3. Lint (mandatory gate): `cargo clippy --workspace --all-targets -- -D warnings`
4. Test: `cargo test --workspace`
5. Format: `cargo fmt --all --check`

All three gates (lint, test, format) must pass locally before pushing.

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
