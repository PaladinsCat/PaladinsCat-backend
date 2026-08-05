-- 038_clean_changelog.sql
-- Clean up stack_versions: remove fake v1.0 entries, remove sensitive v0.2.84,
-- rename versions to 0.1 / 0.2.x scheme, sanitize changelog text.

-- Remove fake backfill row (no commit hash, no changelog)
DELETE FROM stack_versions WHERE id = 1;

-- Remove component rows for v0.1.0-alpha (manual migration, ids 3-8)
DELETE FROM stack_versions WHERE id IN (3, 4, 5, 6, 7, 8);

-- Remove component rows for v0.1.0-alpha first deploy (ids 10-14)
DELETE FROM stack_versions WHERE id IN (10, 11, 12, 13, 14);

-- Remove component rows for v1.0.0-beta (ids 16-20)
DELETE FROM stack_versions WHERE id IN (16, 17, 18, 19, 20);

-- Remove entire v0.2.84 deployment — sensitive internal notes (ids 21-26)
DELETE FROM stack_versions WHERE id IN (21, 22, 23, 24, 25, 26);

-- Remove component rows for v0.2.96 (ids 28-32)
DELETE FROM stack_versions WHERE id IN (28, 29, 30, 31, 32);

-- Rename versions to public scheme
UPDATE stack_versions SET version = 'v0.1.0' WHERE id = 2;
UPDATE stack_versions SET version = 'v0.1.0' WHERE id = 9;
UPDATE stack_versions SET version = 'v0.2.0' WHERE id = 15;
UPDATE stack_versions SET version = 'v0.2.1' WHERE id = 27;

-- Sanitize changelog: remove internal implementation details
UPDATE stack_versions SET changelog = '
- Fixed SSH deploy script authentication for VPS deployments
- Improved deploy reliability with SSH_ASKPASS configuration' WHERE id = 15;

UPDATE stack_versions SET changelog = '
- Fixed deploy script changelog to show all commits since last deployment
- Fixed notification message to remove version number
- Improved deploy script reliability and error handling' WHERE id = 27;
