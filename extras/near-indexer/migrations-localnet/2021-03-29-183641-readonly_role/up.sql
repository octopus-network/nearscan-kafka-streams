DO $$
BEGIN
CREATE ROLE readonly;
EXCEPTION WHEN DUPLICATE_OBJECT THEN
  RAISE NOTICE 'not creating role readonly -- it already exists';
END
$$;
GRANT USAGE ON SCHEMA indexer TO readonly;
GRANT SELECT ON ALL TABLES IN SCHEMA indexer TO readonly;