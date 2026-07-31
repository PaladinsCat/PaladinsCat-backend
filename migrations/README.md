# Database ownership

`bootstrap/` contains the ordered schema and seed scripts used only when
PostgreSQL initializes an empty volume.

`tracked/` contains the only SQL migrations automatically applied to an
existing PaladinsCat database. Use immutable, forward-only names such as
`039_add_match_source.sql`; versions must be unique and monotonically increasing.

Migrations are transactional by default and run with a five-second lock timeout
and ten-minute statement timeout. Put this directive in the first eight lines
only when PostgreSQL forbids a transaction, for example for `CREATE INDEX
CONCURRENTLY`:

```sql
-- paladinscat:transaction=off
```

Non-transactional migrations require an explicit recovery plan. Destructive or
large data transformations require a verified VPS backup and should use the
expand/backfill/switch/contract sequence documented in `documents/deploy.md`.
Mark those files in the first eight lines so routine deploys refuse to apply
them without explicit confirmation:

```sql
-- paladinscat:requires-full-backup
```

Never edit an applied migration. The runner records a SHA-256 checksum and
refuses to continue if an applied file changes.

The emergency `paladinscat-admin storage raw-json` bridge is deliberately not a
tracked migration: production is at migration 111, while applying 112-115 would
also switch non-ranked physical ownership before full compatibility is proven.
The bridge installs reversible future-write guards and performs bounded payload
compaction without advancing `schema_migrations`. Migration 115 remains the
eventual authoritative contract step.
