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
  Package A foundation-read routes, and all 65 Package B analytical routes on
  its private candidate port.
- All 46 routes pass 117 full-application TypeScript-versus-Rust HTTP fixtures
  and their configured durable-table snapshots using the shared production
  middleware/security boundary.
- All 46 routes also pass 117 read-only comparisons against current production
  PostgreSQL data with isolated local Redis and no relay, search, scheduler, or
  worker access. The aggregate report is
  `dev/compat/backend-rust/package-a-shadow-summary.json`.
- The candidate still reports zero production-migrated routes. Cutover remains
  a separate explicitly authorized gate.
- The candidate currently has source implementations for 160 route IDs and
  reports zero production-migrated route IDs. Package B's original 29 routes
  retain disposable fixture parity. All 65 Package B routes now have
  read-only production-data shadow parity; the final 36 pass 38/38 fixtures
  with isolated Redis and the database forced read-only. Those 36 still need
  disposable seeded-database parity before Package B's fixture count can move
  from 29/65 to 65/65.
- All 21 `matches` route IDs now have native candidate handlers and pass the
  current 25-fixture disposable suite and 21-fixture forced-read-only
  production-data shadow. The disposable suite also proves exact raw-response
  audit and live-fallback state across four durable tables. This is
  route-level evidence, not full branch acceptance. The next candidate slice
  now has a transactional shared fact finalizer and a disposable
  DB-miss → relay → durable-facts requested-lookup test. Known partial matches
  now execute missing cached-history reads plus `resumeMatchRecovery`; a live
  disposable trace proves this branch never replays match detail or roster
  operations. The canonical finalizer now also owns the Rust candidate's
  private-account observation, identity scoring/linking, history, and
  resolved/unresolved presence transaction. The existing 13-case TypeScript
  score contract, 8 Rust score cases, and a disposable PostgreSQL lifecycle
  test pass. It still needs an exact TypeScript-versus-Rust full fact-table
  comparison and batched discovery/pull lifecycle traces before this branch
  can be accepted.
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
scripts/migration/Test-PaladinsCatRouteCompatibility.ps1 -RouteGroup package-c-matches
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
