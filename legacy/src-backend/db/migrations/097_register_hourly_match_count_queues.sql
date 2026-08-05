-- Register every queue observed in production player match history so the
-- hourly ID-only discovery worker and public queue reference use the same
-- stable labels. Queue 486 remains the only full ranked ingest queue.
INSERT INTO queue_types (queue_id, queue_name, is_ranked) VALUES
    (424, 'Casual Siege', false),
    (425, 'Siege Training', false),
    (452, 'Casual Onslaught', false),
    (453, 'Onslaught Training', false),
    (469, 'Team Deathmatch', false),
    (486, 'Ranked Siege', true),
    (10297, 'Team Deathmatch Training', false),
    (10332, 'Arcade', false),
    (10348, 'Wave Defense Party Beta', false),
    (10362, 'Wave Defense Public Beta', false),
    (10367, 'Newcomer', false),
    (10369, 'Experiment: Subclasses', false)
ON CONFLICT (queue_id) DO UPDATE SET
    queue_name = EXCLUDED.queue_name,
    is_ranked = EXCLUDED.is_ranked;

COMMENT ON TABLE match_count_discoveries IS
  'Durable ID-only observations from getmatchidsbyqueue for every production-observed queue. Non-ranked rows never enter the full ranked stats ingest pipeline.';
