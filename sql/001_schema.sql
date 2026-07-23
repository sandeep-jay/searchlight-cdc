-- Direct-table experiment schema (single live tables, no delta/nightly split)
-- Placeholders: {rds_schema_boa_app_rds_data}, {rds_app_boa_user}

CREATE SCHEMA IF NOT EXISTS {rds_schema_boa_app_rds_data};

GRANT USAGE
  ON SCHEMA {rds_schema_boa_app_rds_data}
  TO {rds_app_boa_user};

ALTER DEFAULT PRIVILEGES
  IN SCHEMA {rds_schema_boa_app_rds_data}
  GRANT SELECT ON TABLES TO {rds_app_boa_user};

COMMENT ON SCHEMA {rds_schema_boa_app_rds_data} IS
  'Direct-table CDC experiment: advising notes, topics, FTS, audit log';
