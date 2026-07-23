-- Indexes for direct-table experiment

CREATE INDEX advising_notes_sid_idx
  ON {rds_schema_boa_app_rds_data}.advising_notes (sid);

CREATE INDEX advising_notes_boa_id_idx
  ON {rds_schema_boa_app_rds_data}.advising_notes (boa_id);

CREATE INDEX advising_notes_advisor_uid_idx
  ON {rds_schema_boa_app_rds_data}.advising_notes (advisor_uid);

CREATE INDEX advising_notes_updated_at_idx
  ON {rds_schema_boa_app_rds_data}.advising_notes (updated_at);

CREATE INDEX advising_note_topics_sid_idx
  ON {rds_schema_boa_app_rds_data}.advising_note_topics (sid);

CREATE INDEX advising_note_topics_boa_id_idx
  ON {rds_schema_boa_app_rds_data}.advising_note_topics (boa_id);

CREATE INDEX advising_note_topics_topic_idx
  ON {rds_schema_boa_app_rds_data}.advising_note_topics (topic);

CREATE INDEX advising_notes_search_index_fts_index_idx
  ON {rds_schema_boa_app_rds_data}.advising_notes_search_index
  USING gin (fts_index);

CREATE INDEX advising_notes_cdc_log_composite_id_idx
  ON {rds_schema_boa_app_rds_data}.advising_notes_cdc_log (composite_id, received_at);

CREATE INDEX advising_notes_cdc_log_status_idx
  ON {rds_schema_boa_app_rds_data}.advising_notes_cdc_log (apply_status, received_at);
