/// Resolve the public player name using the same contamination guards and
/// fallback order as the TypeScript backend.
///
/// Keep this expression centralized: leaderboard, rating, esports, champion,
/// and player routes all depend on the exact same public-name contract.
pub const DISPLAY_NAME_SQL: &str = r#"COALESCE(
  CASE
    WHEN NULLIF(p.hz_player_name, '') IS NOT NULL
      AND p.hz_player_name !~* '^(DummyPlayer[0-9]+|[0-9a-f]{20,}User-[0-9a-f]{6,})$'
    THEN p.hz_player_name
  END,
  CASE
    WHEN NULLIF(p.hz_gamer_tag, '') IS NOT NULL
      AND p.hz_gamer_tag !~* '^(DummyPlayer[0-9]+|[0-9a-f]{20,}User-[0-9a-f]{6,})$'
    THEN p.hz_gamer_tag
  END,
  CASE
    WHEN NULLIF(p.name, '') IS NOT NULL
      AND p.name !~* '^(DummyPlayer[0-9]+|[0-9a-f]{20,}User-[0-9a-f]{6,})$'
    THEN p.name
  END,
  'Player ' || p.id::text
)"#;
