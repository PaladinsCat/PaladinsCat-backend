# PaladinsCat Migration Consolidation Plan

## Executive Summary

**Current state:** 38 SQL files, 5,886 lines total. `001_schema.sql` (2,513 lines) already consolidates migrations 001–016+ and contains ~101 tables, 12 materialized views, functions, triggers, and seed data. Files 002–037 add incremental changes, most of which are now **fully duplicated** inside 001.

**Proposed state:** 4–5 files total:
- `001_schema.sql` — Keep as-is (already comprehensive)
- `002_extended_schema.sql` — New file: genuinely new tables/columns not yet in 001
- `003_data_migrations.sql` — Ordered data migrations (non-idempotent UPDATEs/DELETEs)
- `004_seed_data.sql` — Pure seed/reference data
- Remove 33 of 37 incremental files (89% reduction)

---

## 1. Duplicate Definitions (Tables/Columns in Both 001 and Later Files)

These files define tables or columns that **already exist identically** in `001_schema.sql`. They are safe to remove because 001 uses `IF NOT EXISTS` throughout.

| File | Duplicated Objects | Notes |
|------|-------------------|-------|
| **002_core_tables.sql** | `matches`, `match_players`, `raw_ingest_buffer`, `hirez_raw_api_responses`, `raw_ingest_buffer_retention_audit`, `match_ingest_status`, `player_match_history_cache`, `player_match_history_entries`, `player_name_history`, `player_account_merges` | **COMPLETE DUPLICATE** — every table already in 001 with identical schemas |
| **004_ingest_completion_status.sql** | `match_ingest_status`, `match_opponent_facts` | Both tables already in 001 |
| **009_player_relationships_alignment.sql** | `player_relationships` + 5 indexes | Already in 001 |
| **010_champion_stats_alignment.sql** | `champion_stats_ranked` | Already in 001 |
| **011_hourly_ingest_state.sql** | `hourly_ingest_state` | Already in 001. Only unique action: `DROP INDEX IF EXISTS idx_hmc_zero_retry` |
| **015_api_key_sync_observability.sql** | `api_keys.last_sync_at`, `api_keys.last_sync_error` | Already in 001 |
| **016_recovery_player_history_cache.sql** | `player_match_history_cache`, `player_match_history_entries`, `idx_rib_match_history_entity_status` | Already in 001 |
| **017_ingest_cleanup_audit.sql** | `ingest_cleanup_audit` | Already in 001 |
| **019_raw_ingest_buffer_retention.sql** | `raw_ingest_buffer_retention_audit` | Already in 001 |
| **024_hourly_ingest_match_debt.sql** | `hourly_ingest_match_debt` | Already in 001 |
| **026_hirez_raw_api_responses.sql** | `hirez_raw_api_responses` | Already in 001 |
| **034_auth_tables.sql** | `users`, `sessions` | Already in 001 (001 has richer schemas) |

### Column-level duplicates (ALTER TABLE ADD COLUMN already in 001)

| File | Duplicated Columns | Target Table |
|------|-------------------|-------------|
| **005_api_key_schema_alignment.sql** | `total_24h`, `daily_limit`, `consecutive_failures`, `last_sync_at`, `last_sync_error` | `api_keys` |
| **006_schema_runtime_alignment.sql** | `region` | `match_players`; `matches_sea`, `matches_jpn`, `matches_rus` | `hourly_match_counts` |
| **008_runtime_ingest_table_alignment.sql** | `cheater`, `sus_count` | `players`; `match_bans` table |
| **012_api_usage_limit_policy.sql** | Same as 005 (duplicate of duplicate) | `api_keys` |
| **025_player_profile_name_guardrails.sql** | `platform_name`, `hz_player_name`, `hz_gamer_tag`, `name_source`, `name_anomaly`, `name_anomaly_reason`, `name_anomaly_detected_at` | `players` |
| **027_player_full_hirez_profile_fields.sql** | `active_player_id`, `ret_msg`, `hirez_profile_refreshed_at`, all `kbm_*`, `controller_*`, `conquest_*` columns | `players`; `player_profile_merged_players` table |
| **028_columnize_player_profile_storage.sql** | `kbm_player_id`, `kbm_ret_msg`, `controller_player_id`, `controller_ret_msg`, `conquest_player_id`, `conquest_ret_msg` | `players`; `player_profile_merged_players` |
| **031_champion_stats_ranked_tier_denominator.sql** | `league_tier_count` | `champion_stats_ranked` |
| **033_dummy_player_name_guardrails.sql** | Same columns as 025 | `players` |
| **035_auth_runtime_alignment.sql** | All `users` and `sessions` columns | `users`, `sessions` |
| **036_stack_versions.sql** | All `stack_versions` columns | `stack_versions` |

---

## 2. Pure Documentation Files (No Schema Changes)

| File | Content | Action |
|------|---------|--------|
| **023_player_average_rating_guardrails.sql** (24 lines) | Only `COMMENT ON COLUMN` and `COMMENT ON TABLE` for `players.avg_*`, `player_queue_ratings`, `player_champion_ratings`, `match_rating_snapshots` | **REMOVE** — comments are already in 001 |

---

## 3. Seed Data Only

| File | Content | Action |
|------|---------|--------|
| **008_seed_cards.sql** (1,024 lines) | `INSERT INTO cards (...) ON CONFLICT DO NOTHING` — 465 card rows | **MOVE** to `004_seed_data.sql` |

Note: `001_schema.sql` also contains seed data inline (queue_types, broken_skins, champions id=0, notifications, site_versions, stack_versions). These should stay in 001 since they're part of the base schema.

---

## 4. Trivial Files (≤20 lines, single column/small change)

| File | Lines | Content | Action |
|------|-------|---------|--------|
| **013_rename_baseline_tracker_job.sql** | 10 | `UPDATE sync_jobs SET job_type='baseline_tracker' WHERE job_type='afk_tracker'` | Merge into data migrations |
| **015_api_key_sync_observability.sql** | 16 | 2 column adds (already in 001) | **REMOVE** |
| **020_site_versions.sql** | 19 | Table + 1-row seed (already in 001) | **REMOVE** |
| **032_player_cheater_status.sql** | 20 | New column + backfill UPDATE | Move to 002 + data migrations |
| **037_changelog.sql** | 17 | New column on stack_versions + backfill | Move to 002 + data migrations |

---

## 5. Domain Grouping

### Ingest Pipeline (buffer, state, audit)
- 003_ingest_guardrails.sql — Data migration (UPDATE duplicates) + indexes (dup)
- 004_ingest_completion_status.sql — Duplicate tables
- 008_runtime_ingest_table_alignment.sql — Duplicate tables/columns
- 011_hourly_ingest_state.sql — Duplicate table + DROP INDEX
- 016_recovery_player_history_cache.sql — Duplicate tables
- 017_ingest_cleanup_audit.sql — Duplicate table
- 019_raw_ingest_buffer_retention.sql — Duplicate table
- 024_hourly_ingest_match_debt.sql — Duplicate table
- 029_player_history_retention.sql — **NEW TABLE** (player_history_retention_audit)

### API Key Management
- 005_api_key_schema_alignment.sql — Duplicate columns + data migration
- 012_api_usage_limit_policy.sql — Duplicate of 005
- 015_api_key_sync_observability.sql — Duplicate columns
- 021_api_key_hourly_usage_normalized.sql — Duplicate table + data migration (archive old wide format, backfill)

### Player Profiles & Names
- 009_player_relationships_alignment.sql — Duplicate table
- 025_player_profile_name_guardrails.sql — Duplicate columns + data migration (fix Epic identifiers)
- 027_player_full_hirez_profile_fields.sql — Duplicate columns + table
- 028_columnize_player_profile_storage.sql — Duplicate columns + data migration (JSON→typed)
- 033_dummy_player_name_guardrails.sql — Duplicate columns + data migration (fix DummyPlayer)

### Match Data & History
- 002_core_tables.sql — Complete duplicate
- 022_player_match_history_entries.sql — Duplicate table + **NEW TABLE** (match_players_prefetch_archive) + data migration
- 030_dropped_matches_tracking.sql — **NEW TABLE** (dropped_matches)

### Champion Stats & Rankings
- 010_champion_stats_alignment.sql — Duplicate table
- 031_champion_stats_ranked_tier_denominator.sql — Duplicate column + data migration
- 032_player_tier_profile_stats.sql — Functions + triggers + data migration

### Auth & Community
- 018_notifications.sql — Duplicate table + data migration (column renames)
- 034_auth_tables.sql — Duplicate tables
- 035_auth_runtime_alignment.sql — Duplicate columns + data migration

### Site Metadata
- 020_site_versions.sql — Duplicate table + seed
- 036_stack_versions.sql — Duplicate table + seed
- 037_changelog.sql — **NEW COLUMN** + data migration

### Rating Guardrails (docs)
- 023_player_average_rating_guardrails.sql — Documentation only

### Seed Data
- 008_seed_cards.sql — Pure seed data

### Scheduler Jobs
- 013_rename_baseline_tracker_job.sql — Data migration

---

## 6. Consolidation Strategy

### What stays in `001_schema.sql` (UNCHANGED)

Keep `001_schema.sql` exactly as-is. It already contains:
- All reference tables (champions, items, maps, etc.)
- All core tables (players, matches, match_players, raw_ingest_buffer, etc.)
- All columns added by files 002–036 (verified above)
- All materialized views
- All functions and triggers (except the tier profile stats trigger from 032)
- All seed data (queue_types, broken_skins, notifications, site_versions, stack_versions)
- TimescaleDB hypertable setup

**Total: 2,513 lines, 101 tables, 12 materialized views**

### What merges into `002_extended_schema.sql` (NEW FILE)

This file contains genuinely **new** schema objects not yet in 001:

```sql
-- ============================================================================
-- 002 Extended Schema — Objects added after 001 consolidation
-- ============================================================================

-- From 029: Player history retention audit
CREATE TABLE IF NOT EXISTS player_history_retention_audit (...);

-- From 030: Dropped matches tracking
CREATE TABLE IF NOT EXISTS dropped_matches (...);

-- From 022: Prefetch archive (created during data migration)
CREATE TABLE IF NOT EXISTS match_players_prefetch_archive (LIKE match_players INCLUDING ALL);

-- From 032_player_cheater_status: Cheater status workflow
ALTER TABLE players ADD COLUMN IF NOT EXISTS cheater_status TEXT;

-- From 037: Changelog on stack_versions
ALTER TABLE stack_versions ADD COLUMN IF NOT EXISTS changelog TEXT;

-- From 032_player_tier_profile_stats: Functions and triggers
CREATE OR REPLACE FUNCTION clamp_tier_stats_bucket(...);
CREATE OR REPLACE FUNCTION bump_profile_tier_stats_bucket(...);
CREATE OR REPLACE FUNCTION sync_profile_tier_stats_from_players(...);
DROP TRIGGER IF EXISTS trg_sync_profile_tier_stats ON players;
CREATE TRIGGER trg_sync_profile_tier_stats ...;

-- Cleanup from 011
DROP INDEX IF EXISTS idx_hmc_zero_retry;
```

**Estimated size: ~300 lines**

### What remains as `003_data_migrations.sql` (ORDERED, NON-IDEMPOTENT)

Data migrations that must run in a specific order and may have side effects:

```sql
-- ============================================================================
-- 003 Data Migrations — Ordered, run once per database
-- ============================================================================

-- 1. From 003: Deduplicate raw_ingest_buffer (must run first, before unique constraint)
UPDATE raw_ingest_buffer SET status='failed', ... WHERE id IN (...);

-- 2. From 005/012: Set api_keys daily_limit defaults
UPDATE api_keys SET daily_limit = CASE WHEN dev_id='2116' THEN 15000 ELSE 7500 END ...;

-- 3. From 013: Rename baseline tracker job type
UPDATE sync_jobs SET job_type='baseline_tracker' WHERE job_type='afk_tracker';

-- 4. From 018: Normalize notifications columns (priority→importance, etc.)
UPDATE notifications SET importance=priority WHERE ...;
-- DROP old columns from notifications

-- 5. From 021: Archive old wide api_key_hourly_usage, backfill normalized
-- Archive old table, create new, backfill from api_log

-- 6. From 022: Move prefetch rows from match_players to player_match_history_entries
INSERT INTO player_match_history_entries (...) SELECT ... FROM match_players WHERE source='prefetch';
INSERT INTO match_players_prefetch_archive SELECT ... FROM match_players WHERE source='prefetch';
DELETE FROM match_players WHERE source='prefetch';

-- 7. From 025: Fix Epic platform identifier names
UPDATE players SET platform_name=..., name_anomaly=TRUE WHERE name ~* '^[0-9a-f]{20,}User-...';
-- Repair from match_players.player_name

-- 8. From 028: Extract JSON columns to typed columns, drop old JSON columns
-- Extract ranked_kbm_raw → kbm_player_id, etc.
-- Extract merged_players_json → player_profile_merged_players
-- DROP old JSON columns

-- 9. From 031: Backfill champion_stats_ranked.league_tier_count
UPDATE champion_stats_ranked SET league_tier_count = ... FROM match_players ...;

-- 10. From 032_player_cheater_status: Backfill cheater_status
UPDATE players SET cheater_status='confirmed' WHERE cheater=TRUE AND cheater_status IS NULL;

-- 11. From 032_player_tier_profile_stats: Backfill kbm_tier, populate tier_stats
UPDATE players SET kbm_tier=... FROM match_players ...;
INSERT INTO tier_stats (...) SELECT ... FROM players ...;

-- 12. From 033: Fix DummyPlayer names
UPDATE players SET platform_name=..., name=... WHERE name ~* '^DummyPlayer[0-9]+$';

-- 13. From 035: Normalize users defaults
UPDATE users SET salt=COALESCE(salt,''), is_admin=COALESCE(is_admin,FALSE), ...;

-- 14. From 036: Backfill stack_versions from site_versions
INSERT INTO stack_versions (...) SELECT ... FROM site_versions ...;

-- 15. From 037: Backfill changelog from notes
UPDATE stack_versions SET changelog=notes WHERE changelog IS NULL AND notes IS NOT NULL;

-- 16. From 014: Refresh materialized view
REFRESH MATERIALIZED VIEW mv_player_coplay_stats;
```

**Estimated size: ~400 lines**

**Ordering constraints:**
- Step 1 (003) must run before any unique constraint enforcement
- Steps 7 (025) and 12 (033) both modify `players.name` — 025 runs first (Epic identifiers), 033 runs second (DummyPlayer)
- Step 8 (028) depends on columns from 027 (already in 001)
- Step 11 (tier profile stats) depends on `kbm_tier` column (already in 001)
- Step 14 (036) depends on `site_versions` table (already in 001)

### What moves to `004_seed_data.sql`

```sql
-- ============================================================================
-- 004 Seed Data — Reference data, safe to re-run
-- ============================================================================

-- From 008_seed_cards.sql: 465 card rows
INSERT INTO cards (card_id, card_name, champion_id) VALUES
(11302, 'Sprint', 2056),
...
ON CONFLICT DO NOTHING;
```

**Size: ~1,024 lines (unchanged from 008_seed_cards.sql)**

### Files to REMOVE (33 files)

| # | File | Reason |
|---|------|--------|
| 1 | `002_core_tables.sql` | Complete duplicate of 001 |
| 2 | `003_ingest_guardrails.sql` | Data migration moved to 003, indexes already in 001 |
| 3 | `004_ingest_completion_status.sql` | Complete duplicate |
| 4 | `005_api_key_schema_alignment.sql` | Columns in 001, data migration moved to 003 |
| 5 | `006_schema_runtime_alignment.sql` | Columns already in 001 |
| 6 | `008_runtime_ingest_table_alignment.sql` | Tables/columns already in 001 |
| 7 | `008_seed_cards.sql` | Moved to 004_seed_data.sql |
| 8 | `009_player_relationships_alignment.sql` | Complete duplicate |
| 9 | `010_champion_stats_alignment.sql` | Complete duplicate |
| 10 | `011_hourly_ingest_state.sql` | Table in 001, DROP INDEX moved to 002 |
| 11 | `012_api_usage_limit_policy.sql` | Duplicate of 005 |
| 12 | `013_rename_baseline_tracker_job.sql` | Data migration moved to 003 |
| 13 | `014_coplay_projection_alignment.sql` | MV in 001, REFRESH moved to 003 |
| 14 | `015_api_key_sync_observability.sql` | Columns already in 001 |
| 15 | `016_recovery_player_history_cache.sql` | Complete duplicate |
| 16 | `017_ingest_cleanup_audit.sql` | Complete duplicate |
| 17 | `018_notifications.sql` | Table in 001, data migration moved to 003 |
| 18 | `019_raw_ingest_buffer_retention.sql` | Complete duplicate |
| 19 | `020_site_versions.sql` | Table + seed already in 001 |
| 20 | `021_api_key_hourly_usage_normalized.sql` | Table in 001, data migration moved to 003 |
| 21 | `022_player_match_history_entries.sql` | Table in 001, archive table moved to 002, data migration moved to 003 |
| 22 | `023_player_average_rating_guardrails.sql` | Documentation only, comments in 001 |
| 23 | `024_hourly_ingest_match_debt.sql` | Complete duplicate |
| 24 | `025_player_profile_name_guardrails.sql` | Columns in 001, data migration moved to 003 |
| 25 | `026_hirez_raw_api_responses.sql` | Complete duplicate |
| 26 | `027_player_full_hirez_profile_fields.sql` | Columns/table already in 001 |
| 27 | `028_columnize_player_profile_storage.sql` | Columns/table in 001, data migration moved to 003 |
| 28 | `029_player_history_retention.sql` | New table moved to 002 |
| 29 | `030_dropped_matches_tracking.sql` | New table moved to 002 |
| 30 | `031_champion_stats_ranked_tier_denominator.sql` | Column in 001, data migration moved to 003 |
| 31 | `032_player_cheater_status.sql` | New column moved to 002, data migration moved to 003 |
| 32 | `032_player_tier_profile_stats.sql` | Functions/triggers moved to 002, data migration moved to 003 |
| 33 | `033_dummy_player_name_guardrails.sql` | Columns in 001, data migration moved to 003 |
| 34 | `034_auth_tables.sql` | Complete duplicate |
| 35 | `035_auth_runtime_alignment.sql` | Columns in 001, data migration moved to 003 |
| 36 | `036_stack_versions.sql` | Table + seed already in 001 |
| 37 | `037_changelog.sql` | New column moved to 002, data migration moved to 003 |

---

## 7. Final File Structure

```
src/backend/db/
├── 001_schema.sql              (2,513 lines) — Base schema (UNCHANGED)
├── 002_extended_schema.sql     (~300 lines)  — New tables, columns, functions, triggers
├── 003_data_migrations.sql     (~400 lines)  — Ordered data migrations
├── 004_seed_data.sql           (~1,024 lines) — Card seed data
└── CONSOLIDATION_PLAN.md       (this file)
```

**Total: 4 SQL files, ~4,237 lines (vs. 38 files, 5,886 lines)**
**Reduction: 38 → 4 files (89% fewer files), 5,886 → ~4,237 lines (28% fewer lines)**

---

## 8. Safety Analysis for Existing VPS Databases

### Why this is safe:

1. **001_schema.sql is idempotent** — All `CREATE TABLE IF NOT EXISTS`, `CREATE INDEX IF NOT EXISTS`, `ALTER TABLE ADD COLUMN IF NOT EXISTS`. Running on an existing VPS database is a no-op for objects that already exist.

2. **002_extended_schema.sql is idempotent** — Same pattern. New tables use `IF NOT EXISTS`, column adds use `IF NOT EXISTS`, functions use `CREATE OR REPLACE`, triggers use `DROP TRIGGER IF EXISTS` before `CREATE TRIGGER`.

3. **003_data_migrations.sql contains the risk** — The UPDATE/DELETE/REFRESH statements are NOT idempotent in the traditional sense, but they are safe to re-run because:
   - All UPDATEs use WHERE clauses that only match rows needing migration
   - The `cheater_status` backfill checks `WHERE cheater_status IS NULL`
   - The `daily_limit` UPDATE uses `WHERE daily_limit IS NULL OR daily_limit <> expected`
   - The `api_key_hourly_usage` archive checks for old wide columns before migrating
   - The `notifications` column renames check for old column existence before acting
   - The `stack_versions` backfill checks `WHERE NOT EXISTS`
   - The `changelog` backfill checks `WHERE changelog IS NULL`

4. **004_seed_data.sql uses `ON CONFLICT DO NOTHING`** — Safe to re-run.

### Execution order on fresh database:
1. `001_schema.sql` — Creates all base tables
2. `002_extended_schema.sql` — Adds new tables/columns/functions/triggers
3. `003_data_migrations.sql` — Populates/transforms data (no-op on empty DB)
4. `004_seed_data.sql` — Inserts seed data

### Execution order on existing VPS database (theoretical, not used):
Since docker-entrypoint-initdb.d only runs on first startup, this scenario doesn't apply. But if someone ran these manually, the idempotent design ensures safety.

---

## 9. Implementation Checklist

- [ ] Create `002_extended_schema.sql` with new schema objects from files 022, 029, 030, 032, 032_player_tier_profile_stats, 037
- [ ] Create `003_data_migrations.sql` with ordered data migrations from files 003, 005, 012, 013, 014, 018, 021, 022, 025, 028, 031, 032, 033, 035, 036, 037
- [ ] Rename `008_seed_cards.sql` → `004_seed_data.sql`
- [ ] Delete 33 obsolete migration files (list above)
- [ ] Verify 001_schema.sql already contains all objects from 002–036
- [ ] Test on fresh PostgreSQL 18 + TimescaleDB 2.26 container
- [ ] Verify no breaking changes to existing VPS databases (idempotent check)
