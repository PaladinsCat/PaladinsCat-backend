-- paladinscat:requires-full-backup
-- GitHub pull requests now own translation storage and review. Remove the
-- obsolete VPS draft, token, submission, and approval data after backup.

DROP TABLE IF EXISTS localization_submissions;
DROP TABLE IF EXISTS localization_api_tokens;
DROP TABLE IF EXISTS localization_pull_requests;
DROP TABLE IF EXISTS localization_drafts;
DROP TABLE IF EXISTS localization_access_requests;
DROP TABLE IF EXISTS localization_contributors;
