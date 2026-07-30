# PaladinsCat native backend

This crate is the in-progress Rust replacement for the TypeScript API, workers,
and production operator commands.

The target is a complete replacement, not a hybrid steady state. The fixed
completion ledgers are 268 HTTP routes, 41 worker modules, 6 scheduler owners,
114 environment-variable contracts, every production operator command, and
every backend deployment selector. The TypeScript runtime is removed only after
full parity, an authorized cutover, and the bounded production soak.

It is intentionally not a production backend yet:

- `paladinscat-api` is disabled unless
  `PALADINSCAT_RUST_CANDIDATE_ENABLE=true` is explicitly set.
- It exposes `/health`, `/deployment/status`, `/migration/status`, all 46
  Package A foundation-read routes, and the 28 implemented Package B routes
  (`coplay`, `meta`, and the first 11 `stats` routes) on its private candidate
  port.
- All 46 routes pass 117 full-application TypeScript-versus-Rust HTTP fixtures
  and their configured durable-table snapshots using the shared production
  middleware/security boundary.
- All 46 routes also pass 117 read-only comparisons against current production
  PostgreSQL data with isolated local Redis and no relay, search, scheduler, or
  worker access. The aggregate report is
  `dev/compat/backend-rust/package-a-shadow-summary.json`.
- The candidate still reports zero production-migrated routes. Cutover remains
  a separate explicitly authorized gate.
- The candidate currently reports 76 implemented route IDs and zero migrated
  route IDs. The two additional system routes still require full-stack
  dependency and shadow parity; `coplay` passes 26/26 exact HTTP fixtures and
  `meta` passes 38/38 exact HTTP fixtures plus read-only production-data
  comparison. The first 11 `stats` routes pass 61/61 exact HTTP/cache fixtures,
  their configured durable-table snapshots, and 61/61 read-only
  production-data shadow fixtures.
- `paladinscat-worker` exits without claiming any scheduler ownership.
- Compose continues to select the TypeScript backend until compatibility gates
  and the production cutover are explicitly approved.

The fixed denominator is generated with:

```powershell
cd src/backend
npm run migration:inventory
```

Workspace validation:

```powershell
cargo fmt --all --check
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
```

The first black-box route slice is reproducible with:

```powershell
scripts/migration/Test-PaladinsCatRouteCompatibility.ps1 -RouteGroup recovery
scripts/migration/Test-PaladinsCatRouteCompatibility.ps1 -RouteGroup notifications
scripts/migration/Test-PaladinsCatRouteCompatibility.ps1 -RouteGroup esports
scripts/migration/Test-PaladinsCatRouteCompatibility.ps1 -RouteGroup ratings
scripts/migration/Test-PaladinsCatRouteCompatibility.ps1 -RouteGroup reference
scripts/migration/Test-PaladinsCatRouteCompatibility.ps1 -RouteGroup public-operations
scripts/migration/Test-PaladinsCatRouteCompatibility.ps1 -RouteGroup coplay
scripts/migration/Test-PaladinsCatRouteCompatibility.ps1 -RouteGroup meta
scripts/migration/Test-PaladinsCatRouteCompatibility.ps1 -RouteGroup stats-foundation
scripts/migration/Test-PaladinsCatRouteCompatibility.ps1 -RouteGroup stats-summaries
scripts/migration/Test-PaladinsCatRouteCompatibility.ps1 -RouteGroup foundation
```

Those commands create disposable PostgreSQL and Redis instances, start the
current TypeScript route plugin and Rust candidate on private loopback ports,
run the inventory-bound HTTP and database comparators, write machine-readable
reports, and clean up.

The full phase plan is
`documents/02-technical/backend-rust-migration.md`.
Its machine-checked route and worker packages are
`documents/02-technical/migration/backend-rust-work-packages.json`.
