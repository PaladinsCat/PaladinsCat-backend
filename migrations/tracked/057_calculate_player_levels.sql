-- Preserve the raw Hi-Rez level for diagnostics. The provider caps it at 999,
-- so public player levels are derived from the still-growing Total_XP value.
ALTER TABLE players
  ADD COLUMN IF NOT EXISTS api_level INT NOT NULL DEFAULT 0;

-- Existing rows predate api_level. Preserve their former API-sourced value
-- before replacing the public level with the XP-derived value below.
UPDATE players
SET api_level = level
WHERE api_level = 0
  AND level > 0;

-- This exactly mirrors aRez's calculated_level implementation:
-- levels 2–50 cost 40k, 60k, ..., 1M XP; every level after 50 costs 1M XP.
WITH calculated_levels AS (
  SELECT
    p.id,
    CASE
      WHEN p.total_xp < 25480000 THEN COALESCE((
        SELECT MAX(level)
        FROM generate_series(2, 50) AS levels(level)
        WHERE ((level * (level + 1) / 2) - 1) * 20000 <= p.total_xp
      ), 1)
      ELSE ((p.total_xp - 25480000) / 1000000)::INT + 50
    END AS level
  FROM players p
  -- Match-only rows use the schema's zero default and have never received a
  -- full getplayer profile. Do not turn those unknown values into level 1.
  WHERE p.hirez_profile_refreshed_at IS NOT NULL
    AND p.total_xp IS NOT NULL
    AND p.total_xp >= 0
)
UPDATE players p
SET level = calculated_levels.level,
    last_updated = now()
FROM calculated_levels
WHERE p.id = calculated_levels.id
  AND p.level IS DISTINCT FROM calculated_levels.level;
