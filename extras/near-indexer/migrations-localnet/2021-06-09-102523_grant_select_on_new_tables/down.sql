-- It does not affect privileges assigned to already-existing objects.
-- Source: https://www.postgresql.org/docs/current/sql-alterdefaultprivileges.html
ALTER DEFAULT PRIVILEGES IN SCHEMA indexer REVOKE SELECT ON TABLES FROM readonly;
