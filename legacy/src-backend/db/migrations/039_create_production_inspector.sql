DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'paladinscat_inspector') THEN
    CREATE ROLE paladinscat_inspector
      LOGIN
      NOSUPERUSER
      NOCREATEDB
      NOCREATEROLE
      NOREPLICATION
      NOBYPASSRLS
      CONNECTION LIMIT 3
      PASSWORD NULL;
  END IF;
END
$$;

ALTER ROLE paladinscat_inspector
  NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS
  CONNECTION LIMIT 3 PASSWORD NULL;
ALTER ROLE paladinscat_inspector SET default_transaction_read_only = on;
ALTER ROLE paladinscat_inspector SET statement_timeout = '30s';
ALTER ROLE paladinscat_inspector SET idle_in_transaction_session_timeout = '60s';
GRANT pg_read_all_data TO paladinscat_inspector;

COMMENT ON ROLE paladinscat_inspector IS
  'Passwordless container-local production inspection role; forced read-only defaults and no write/DDL privileges';
