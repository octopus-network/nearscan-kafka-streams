DO $$
BEGIN
CREATE ROLE readonly;
EXCEPTION WHEN DUPLICATE_OBJECT THEN
  RAISE NOTICE 'not creating role readonly -- it already exists';
END
$$;
GRANT USAGE ON SCHEMA localnet TO readonly;
GRANT SELECT ON ALL TABLES IN SCHEMA localnet TO readonly;
