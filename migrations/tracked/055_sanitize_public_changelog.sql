-- The changelog is public product release notes. Remove any historical lines
-- that expose SSH/operator-access implementation details while retaining other
-- user-facing notes in the same release entry.
UPDATE stack_versions
SET changelog = NULLIF(
  trim(
    regexp_replace(
      changelog,
      '^[^\n]*(ssh|openssh|ssh_askpass)[^\n]*(\n|$)',
      '',
      'gin'
    )
  ),
  ''
)
WHERE component = 'stack'
  AND changelog ~* '\m(ssh|openssh|ssh_askpass)\M';
