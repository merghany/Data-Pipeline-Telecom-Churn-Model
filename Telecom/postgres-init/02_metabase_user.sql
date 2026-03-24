-- Runs automatically on first Postgres container startup (after 01_raw_schema.sql).

-- ── Metabase internal app database ───────────────────────────
-- Metabase stores its own metadata (questions, dashboards, users) here.
SELECT 'CREATE DATABASE metabase_app OWNER dbt_user'
    WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'metabase_app')\gexec

-- ── Read-only Metabase analytics user ─────────────────────────
CREATE USER metabase_user WITH PASSWORD 'metabase_pass';

-- Grant connect + usage on the analytics database and marts schema
GRANT CONNECT ON DATABASE telecom_dbt TO metabase_user;
GRANT USAGE   ON SCHEMA marts        TO metabase_user;
GRANT USAGE   ON SCHEMA staging      TO metabase_user;
GRANT USAGE   ON SCHEMA intermediate TO metabase_user;

-- Grant SELECT on all current and future tables in marts
GRANT SELECT ON ALL TABLES IN SCHEMA marts        TO metabase_user;
GRANT SELECT ON ALL TABLES IN SCHEMA staging      TO metabase_user;
GRANT SELECT ON ALL TABLES IN SCHEMA intermediate TO metabase_user;

-- Ensure future tables created by dbt are also readable
ALTER DEFAULT PRIVILEGES IN SCHEMA marts
    GRANT SELECT ON TABLES TO metabase_user;

ALTER DEFAULT PRIVILEGES IN SCHEMA staging
    GRANT SELECT ON TABLES TO metabase_user;

ALTER DEFAULT PRIVILEGES IN SCHEMA intermediate
    GRANT SELECT ON TABLES TO metabase_user;
