CREATE TABLE notifications (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMPTZ NOT NULL DEFAULT now(),
    importance INT NOT NULL DEFAULT 0,
    message TEXT NOT NULL
);

INSERT INTO notifications (id, timestamp, importance, message)
VALUES
  (1, '2026-07-01T01:02:03.456Z', 10, 'Highest priority fixture'),
  (2, '2026-07-02T02:03:04.567Z', 5, 'Newer medium fixture'),
  (3, '2026-07-01T02:03:04.567Z', 5, 'Older medium fixture');
SELECT setval('notifications_id_seq', 3);
