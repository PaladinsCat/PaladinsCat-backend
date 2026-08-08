# PaladinsCat HirezRelay Rust service

This crate is the complete native replacement for the TypeScript HirezRelay
container. Normal Compose builds this implementation and retains the unchanged
service identity `hirezrelay:3015`.

Production deployment is separately gated by a quiesced candidate, an atomic
single-owner transition, postflight verification, an immutable stopped
TypeScript rollback image, and initial, middle, and final production acceptance
checks. Building or testing this
crate does not authorize a production cutover.

The full operation inventory, compatibility gates, rollout, and rollback
requirements are documented in
`documents/02-technical/api/hirez-relay-rust-migration.md`.

## Runtime contract

The service owns:

- `GET /health`
- `GET /metrics`
- `POST /v1/call`
- all 37 shared-manifest operations
- all 35 real-mode handlers
- both dummy-only scenario-control operations
- direct Hi-Rez signing, sessions, retries, key rotation, reserves, accounting,
  and scheduled usage reconciliation
- PostgreSQL local-first recovery and durable history/profile/raw-buffer writes
- Redis health and compatible cache behavior
- completed-match direct, ranked recovery, non-ranked roster-only recovery,
  limited, recovery-pending, and dropped outcomes
- attribution, bounded traces, metrics, readiness, quiesced startup,
  single-owner lease, graceful drain, and dependency shutdown

Both implementations consume the checked-in contract at
`legacy/src-backend/contracts/hirez-relay-operation-contract.json`. The Rust process
validates it at startup, and the differential suites execute every declared
operation.

## Match ownership boundary

For a completed-match request the relay accepts 1–10
`{ matchId, queueId? }` entries and performs one direct batch call first.
Complete direct matches return without recovery. A known non-ranked singleton
may perform the single roster recovery allowed by policy. A ranked or
unknown-queue singleton may run the full recovery path inside the relay.

Ordered continuous batching remains worker-owned: the backend checkpoints
returned outcomes, isolates an omitted blocker through the same canonical
operation, and refills the next ten-ID window. No backend caller signs a Hi-Rez
request or triggers a second recovery implementation.

## Current verification

The current source has passed:

- 49/49 exact dummy-mode TypeScript-versus-Rust operation, recovery, and HTTP
  boundary scenarios
- 41/41 deterministic real-mode scenarios
- 86 ordinary plus 5 PostgreSQL-gated relay tests
- 22 ordinary plus 1 Redis-gated shared-core tests
- exact PostgreSQL key, audit, raw-buffer, history, and profile side effects
- quiesced candidate with zero outbound calls
- owner-collision rejection
- in-flight SIGTERM drain, durable usage flush, and owner reacquisition
- warning-free Clippy and release-container builds

The resource comparison and its reproduction commands are in
`dev/benchmarks/relay-rust-resource-comparison.md`.

## Local execution

Dummy mode is dependency-free:

```powershell
$env:HIREZ_RELAY_MODE = 'dummy'
cargo run --package paladinscat-hirez-relay
```

The binary listens on `127.0.0.1:3015` by default. Use another port when a
local relay already owns `3015`:

```powershell
$env:HIREZ_RELAY_PORT = '3016'
cargo run --package paladinscat-hirez-relay
```

Real mode requires `DATABASE_URL`, `REDIS_URL`, `MEK` or `MEK_FILE`, and an
encrypted database key row or `HIREZ_API_KEYS_FILE`. Production also requires
the explicit value `HIREZ_RELAY_MODE=real`; the process refuses an implicit
dummy fallback when `NODE_ENV=production`.
