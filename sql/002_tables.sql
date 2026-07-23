-- Direct single-table model for CDC experiment

DROP TABLE IF EXISTS {rds_schema_boa_app_rds_data}.advising_notes_cdc_log CASCADE;
DROP TABLE IF EXISTS {rds_schema_boa_app_rds_data}.advising_notes_search_index CASCADE;
DROP TABLE IF EXISTS {rds_schema_boa_app_rds_data}.advising_note_topics_pending CASCADE;
DROP TABLE IF EXISTS {rds_schema_boa_app_rds_data}.advising_note_topics CASCADE;
DROP TABLE IF EXISTS {rds_schema_boa_app_rds_data}.advising_notes CASCADE;

CREATE TABLE {rds_schema_boa_app_rds_data}.advising_notes (
    id VARCHAR PRIMARY KEY,
    sid VARCHAR NOT NULL,
    boa_id VARCHAR NOT NULL,
    advisor_uid VARCHAR,
    author_name VARCHAR,
    advisor_first_name VARCHAR,
    advisor_last_name VARCHAR,
    subject VARCHAR,
    note_body TEXT,
    is_private BOOLEAN,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL,
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL
);

COMMENT ON TABLE {rds_schema_boa_app_rds_data}.advising_notes IS
  'Live advising notes table (direct CDC experiment; bulk-refreshed from export)';

CREATE TABLE {rds_schema_boa_app_rds_data}.advising_note_topics (
    id VARCHAR NOT NULL,
    sid VARCHAR NOT NULL,
    boa_id VARCHAR NOT NULL,
    topic VARCHAR NOT NULL,
    PRIMARY KEY (id, topic)
);

COMMENT ON TABLE {rds_schema_boa_app_rds_data}.advising_note_topics IS
  'Live note topics (direct CDC experiment)';

CREATE TABLE {rds_schema_boa_app_rds_data}.advising_note_topics_pending (
    boa_id VARCHAR NOT NULL,
    topic VARCHAR NOT NULL,
    author_uid VARCHAR,
    received_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    PRIMARY KEY (boa_id, topic)
);

COMMENT ON TABLE {rds_schema_boa_app_rds_data}.advising_note_topics_pending IS
  'Orphan topics parked until parent note arrives (direct CDC experiment)';

CREATE TABLE {rds_schema_boa_app_rds_data}.advising_notes_search_index (
    id VARCHAR PRIMARY KEY,
    fts_index TSVECTOR
);

COMMENT ON TABLE {rds_schema_boa_app_rds_data}.advising_notes_search_index IS
  'FTS index for direct-table CDC experiment';

CREATE TABLE {rds_schema_boa_app_rds_data}.advising_notes_cdc_log (
    log_id BIGSERIAL PRIMARY KEY,
    received_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    applied_at TIMESTAMP WITH TIME ZONE,
    event_id VARCHAR NOT NULL UNIQUE,
    table_name VARCHAR NOT NULL,
    operation VARCHAR NOT NULL,
    effective_operation VARCHAR NOT NULL,
    boa_id VARCHAR,
    composite_id VARCHAR,
    payload JSONB NOT NULL,
    prepared_record JSONB,
    apply_status VARCHAR NOT NULL,
    error_message TEXT,
    handler_version VARCHAR
);

COMMENT ON TABLE {rds_schema_boa_app_rds_data}.advising_notes_cdc_log IS
  'Audit log for direct-table CDC events (replay and integrity checks)';

-- Optional passthrough views for app compatibility during shadow testing
CREATE OR REPLACE VIEW {rds_schema_boa_app_rds_data}.advising_notes_vw AS
  SELECT * FROM {rds_schema_boa_app_rds_data}.advising_notes;

CREATE OR REPLACE VIEW {rds_schema_boa_app_rds_data}.advising_note_topics_vw AS
  SELECT * FROM {rds_schema_boa_app_rds_data}.advising_note_topics;

CREATE OR REPLACE VIEW {rds_schema_boa_app_rds_data}.advising_notes_search_index_vw AS
  SELECT id, fts_index FROM {rds_schema_boa_app_rds_data}.advising_notes_search_index;
