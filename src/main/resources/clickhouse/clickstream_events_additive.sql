-- Additive migration for an existing clickstream_events table.
-- Do not DROP deprecated columns. Apply before a data-ingestor build that inserts the new columns.
-- Deprecated, still present: click_relative_x, click_relative_y, scroll_time_to_25, scroll_time_to_50,
-- scroll_time_to_75, scroll_time_to_90, scroll_time_to_100, engagement_exit_reason.
-- TTL is not set: raw clickstream retention period is not defined.
-- Sort key and monthly partitioning stay unchanged. No skipping indexes are added without measured EXPLAIN results.

ALTER TABLE clickstream_events ADD COLUMN IF NOT EXISTS tab_id Int64 DEFAULT 0;
ALTER TABLE clickstream_events ADD COLUMN IF NOT EXISTS session_started_ts Int64 DEFAULT 0;
ALTER TABLE clickstream_events ADD COLUMN IF NOT EXISTS page_view_started_ts Int64 DEFAULT 0;
ALTER TABLE clickstream_events ADD COLUMN IF NOT EXISTS page_view_index UInt16 DEFAULT 0;
ALTER TABLE clickstream_events ADD COLUMN IF NOT EXISTS event_revision UInt16 DEFAULT 1;
ALTER TABLE clickstream_events ADD COLUMN IF NOT EXISTS event_final Int8 DEFAULT -1;
ALTER TABLE clickstream_events ADD COLUMN IF NOT EXISTS click_target_x Float32 DEFAULT -1;
ALTER TABLE clickstream_events ADD COLUMN IF NOT EXISTS click_target_y Float32 DEFAULT -1;
ALTER TABLE clickstream_events ADD COLUMN IF NOT EXISTS click_viewport_x_ratio Float32 DEFAULT -1;
ALTER TABLE clickstream_events ADD COLUMN IF NOT EXISTS click_viewport_y_ratio Float32 DEFAULT -1;
ALTER TABLE clickstream_events ADD COLUMN IF NOT EXISTS click_document_x_ratio Float32 DEFAULT -1;
ALTER TABLE clickstream_events ADD COLUMN IF NOT EXISTS click_document_y_ratio Float32 DEFAULT -1;
ALTER TABLE clickstream_events ADD COLUMN IF NOT EXISTS scroll_time_to_milestone Int64 DEFAULT -1;
ALTER TABLE clickstream_events ADD COLUMN IF NOT EXISTS engagement_trigger LowCardinality(String) DEFAULT '';
ALTER TABLE clickstream_events ADD COLUMN IF NOT EXISTS browser_name LowCardinality(String) DEFAULT '';
ALTER TABLE clickstream_events ADD COLUMN IF NOT EXISTS browser_version LowCardinality(String) DEFAULT '';
ALTER TABLE clickstream_events ADD COLUMN IF NOT EXISTS browser_major_version UInt16 DEFAULT 0;
ALTER TABLE clickstream_events ADD COLUMN IF NOT EXISTS os_name LowCardinality(String) DEFAULT '';
ALTER TABLE clickstream_events ADD COLUMN IF NOT EXISTS os_version LowCardinality(String) DEFAULT '';
ALTER TABLE clickstream_events ADD COLUMN IF NOT EXISTS device_type LowCardinality(String) DEFAULT '';
ALTER TABLE clickstream_events ADD COLUMN IF NOT EXISTS page_query_filtered String DEFAULT '';
ALTER TABLE clickstream_events ADD COLUMN IF NOT EXISTS target_href_query_filtered String DEFAULT '';
ALTER TABLE clickstream_events ADD COLUMN IF NOT EXISTS target_element_key String DEFAULT '';
ALTER TABLE clickstream_events ADD COLUMN IF NOT EXISTS target_element_type LowCardinality(String) DEFAULT '';
ALTER TABLE clickstream_events ADD COLUMN IF NOT EXISTS target_action_type LowCardinality(String) DEFAULT '';
ALTER TABLE clickstream_events ADD COLUMN IF NOT EXISTS target_semantic_role LowCardinality(String) DEFAULT '';
ALTER TABLE clickstream_events ADD COLUMN IF NOT EXISTS target_identity_source LowCardinality(String) DEFAULT '';
ALTER TABLE clickstream_events ADD COLUMN IF NOT EXISTS target_form_id String DEFAULT '';
