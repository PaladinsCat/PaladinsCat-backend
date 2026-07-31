-- KDA is (kills + assists / 2) / deaths. Deathless rows use a denominator of
-- one so consumers receive a finite, useful value instead of NULL/Infinity.
UPDATE match_players
SET kda = ROUND(
  (COALESCE(kills, 0) + COALESCE(assists, 0) / 2.0)::NUMERIC
  / GREATEST(COALESCE(deaths, 0), 1),
  2
)::DOUBLE PRECISION
WHERE kda IS DISTINCT FROM ROUND(
  (COALESCE(kills, 0) + COALESCE(assists, 0) / 2.0)::NUMERIC
  / GREATEST(COALESCE(deaths, 0), 1),
  2
)::DOUBLE PRECISION;

UPDATE champion_stats_ranked
SET kda = ROUND(
  (COALESCE(sum_kills, 0) + COALESCE(sum_assists, 0) / 2.0)::NUMERIC
  / GREATEST(COALESCE(sum_deaths, 0), 1),
  2
);
