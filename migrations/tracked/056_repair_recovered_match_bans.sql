-- Restore draft bans verified by the durable getdemodetails audit for match
-- 1280775331 on 2026-07-14 (audit id 28921, SHA-256
-- e0211490c48427d02de2d927c9d1e6aa7e7e931e0ab160ab56f3807df582fba7).
--
-- This migration repairs only the per-match source fact. Aggregate ban
-- projections remain derived from match_bans and can be rebuilt by the normal
-- derived-projection repair worker without another Hi-Rez call.
WITH audited_bans (ban_slot, champion_id) AS (
  VALUES
    (1::smallint, 2092), -- Cassie
    (2::smallint, 2479), -- Khan
    (3::smallint, 2094), -- Evie
    (4::smallint, 2477)  -- Terminus
)
INSERT INTO match_bans (match_id, ban_slot, champion_id)
SELECT 1280775331, audited_bans.ban_slot, audited_bans.champion_id
FROM audited_bans
WHERE EXISTS (SELECT 1 FROM matches WHERE match_id = 1280775331)
  AND EXISTS (SELECT 1 FROM champions WHERE id = audited_bans.champion_id)
ON CONFLICT (match_id, ban_slot) DO UPDATE
SET champion_id = EXCLUDED.champion_id;

-- The same synthetic recovery handoff made this match look direct even though
-- all ten durable player facts are source='recovered'. Repair only when that
-- exact local fact shape is still present; recovery API-call counts cannot be
-- reconstructed safely, so no recovery_stats row is fabricated here.
UPDATE matches
SET broken = true,
    recovered = true,
    source = 'recovery'
WHERE match_id = 1280775331
  AND (
    SELECT COUNT(*)
    FROM match_players
    WHERE match_players.match_id = matches.match_id
      AND match_players.source = 'recovered'
  ) = 10;
