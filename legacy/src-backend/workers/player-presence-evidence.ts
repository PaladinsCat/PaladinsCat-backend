/**
 * Shared rolling-presence authority.
 *
 * The discovery ledger defines which matches belong to the rolling window;
 * persisted player facts define which public identities can be evidenced.
 * Every public activity endpoint and cache reconciliation must use this CTE so
 * the overview cannot drift away from the auditable evidence page.
 *
 * Callers must bind PostgreSQL parameter $1 as either a queue ID or NULL.
 */
export const PUBLIC_PLAYER_EVIDENCE_CTES_SQL = `
recent_discoveries AS MATERIALIZED (
  SELECT
    d.match_id,
    d.queue_id,
    q.queue_name,
    q.stats_scope,
    q.participant_model,
    (
      COALESCE(
        d.entry_datetime AT TIME ZONE 'UTC',
        d.source_date + (d.source_hour * interval '1 hour')
      ) AT TIME ZONE 'UTC'
    ) AS observed_at
  FROM match_count_discoveries d
  JOIN queue_types q ON q.queue_id = d.queue_id
  WHERE d.source_date >= ((now() AT TIME ZONE 'UTC') - interval '25 hours')::date
    AND COALESCE(
          d.entry_datetime AT TIME ZONE 'UTC',
          d.source_date + (d.source_hour * interval '1 hour')
        ) >= (now() AT TIME ZONE 'UTC') - interval '24 hours'
    AND q.track_presence = TRUE
    AND ($1::int IS NULL OR d.queue_id = $1::int)
),
roster_evidence AS MATERIALIZED (
  SELECT
    mp.player_id,
    mp.match_id,
    discovery.queue_id,
    discovery.queue_name,
    discovery.stats_scope,
    discovery.observed_at,
    NULLIF(BTRIM(mp.player_name), '') AS observed_name,
    CASE
      WHEN mp.player_id > 0 THEN 'human'
      WHEN UPPER(COALESCE(mp.player_name, '')) = 'PRIVATEACCOUNT'
        OR COALESCE(mp.private_slot, 0) > 0
        THEN 'private'
      ELSE 'unknown'
    END AS participant_kind
  FROM recent_discoveries discovery
  JOIN match_players mp ON mp.match_id = discovery.match_id
  WHERE mp.entry_datetime >= now() - interval '25 hours'
    AND COALESCE(mp.source, 'direct') IN ('direct', 'recovered')

  UNION ALL

  SELECT
    cmp.player_id,
    cmp.match_id,
    discovery.queue_id,
    discovery.queue_name,
    discovery.stats_scope,
    discovery.observed_at,
    NULLIF(BTRIM(cmp.player_name), '') AS observed_name,
    COALESCE(cmp.participant_kind, 'human') AS participant_kind
  FROM recent_discoveries discovery
  JOIN casual_match_players cmp ON cmp.match_id = discovery.match_id

  UNION ALL

  SELECT
    smp.player_id,
    smp.match_id,
    discovery.queue_id,
    discovery.queue_name,
    discovery.stats_scope,
    discovery.observed_at,
    NULLIF(BTRIM(smp.player_name), '') AS observed_name,
    COALESCE(smp.participant_kind, 'human') AS participant_kind
  FROM recent_discoveries discovery
  JOIN special_match_players smp ON smp.match_id = discovery.match_id
),
participation AS MATERIALIZED (
  SELECT
    player_id,
    match_id,
    queue_id,
    queue_name,
    stats_scope,
    observed_at,
    observed_name
  FROM roster_evidence
  WHERE player_id > 0
    AND participant_kind = 'human'
),
roster_summary AS MATERIALIZED (
  SELECT
    match_id,
    COUNT(DISTINCT player_id) FILTER (
      WHERE player_id > 0 AND participant_kind = 'human'
    )::int AS known_public_players,
    COUNT(*) FILTER (
      WHERE player_id <= 0 AND participant_kind <> 'bot'
    )::int AS observed_unresolved_slots,
    COUNT(*) FILTER (
      WHERE participant_kind <> 'bot'
    )::int AS observed_human_slots
  FROM roster_evidence
  GROUP BY match_id
),
durable_fact_candidates AS MATERIALIZED (
  SELECT
    discovery.match_id,
    COALESCE(status.status = 'complete', false) AS facts_complete
  FROM recent_discoveries discovery
  LEFT JOIN match_ingest_status status ON status.match_id = discovery.match_id
  WHERE discovery.queue_id = 486

  UNION ALL

  SELECT
    discovery.match_id,
    acquisition.status = 'complete_direct' AS facts_complete
  FROM recent_discoveries discovery
  JOIN nonranked_match_acquisition acquisition
    ON acquisition.match_id = discovery.match_id
  WHERE discovery.queue_id <> 486
),
fact_completeness AS MATERIALIZED (
  SELECT match_id, BOOL_OR(facts_complete) AS facts_complete
  FROM durable_fact_candidates
  GROUP BY match_id
),
match_uncertainty AS MATERIALIZED (
  SELECT
    discovery.match_id,
    discovery.queue_id,
    discovery.queue_name,
    discovery.stats_scope,
    CASE
      -- Training and PvE teams are not filled to capacity: a solo player can
      -- start one of these matches. Empty human capacity is therefore not
      -- evidence of an unresolved participant. Count only unresolved roster
      -- rows that Hi-Rez actually reported.
      WHEN discovery.participant_model IN ('bots', 'pve')
        THEN COALESCE(roster.observed_unresolved_slots, 0)
      WHEN COALESCE(completeness.facts_complete, false)
        AND COALESCE(roster.observed_human_slots, 0) > 0
        THEN COALESCE(roster.observed_unresolved_slots, 0)
      ELSE GREATEST(
        10 - COALESCE(roster.known_public_players, 0),
        COALESCE(roster.observed_unresolved_slots, 0),
        0
      )
    END::int AS unresolved_slots_upper
  FROM recent_discoveries discovery
  LEFT JOIN roster_summary roster ON roster.match_id = discovery.match_id
  LEFT JOIN fact_completeness completeness ON completeness.match_id = discovery.match_id
)`;
