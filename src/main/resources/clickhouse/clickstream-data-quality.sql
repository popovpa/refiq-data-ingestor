-- Diagnostic queries only. Not used by tracker or data-ingestor at runtime.
-- raw clickstream TTL is not configured because retention period is not defined.

-- 1. counts by event_type
SELECT event_type, count() AS events
FROM clickstream_events
WHERE captured_ts > 0
GROUP BY event_type
ORDER BY events DESC;

-- 2. missing ids
SELECT
    countIf(client_event_id = 0) AS events_without_client_event_id,
    countIf(session_id = 0) AS events_without_session_id,
    countIf(page_view_id = 0) AS events_without_page_view_id,
    countIf(visitor_id = 0) AS events_without_visitor_id,
    countIf(site_id = 0) AS events_without_site_id
FROM clickstream_events;

-- 3. duplicate client_event_id
SELECT site_key, client_event_id, count() AS copies
FROM clickstream_events
WHERE client_event_id != 0
GROUP BY site_key, client_event_id
HAVING copies > 1
ORDER BY copies DESC
LIMIT 100;

-- 4. sequence gaps inside a session
SELECT site_key, session_id, sequence, neighbor(sequence, 1) AS next_sequence
FROM (
    SELECT site_key, session_id, sequence
    FROM clickstream_events
    WHERE session_id != 0
    ORDER BY site_key, session_id, sequence
)
WHERE next_sequence > sequence + 1
LIMIT 100;

-- 5. page_view context completeness
SELECT
    countIf(page_view_id = 0) AS missing_page_view,
    countIf(page_view_started_ts = 0) AS missing_page_start,
    countIf(page_hostname = '') AS missing_hostname,
    countIf(page_pathname = '') AS missing_pathname
FROM clickstream_events
WHERE event_type = 'page_view';

-- 6. attribution completeness by event type
SELECT
    event_type,
    count() AS events,
    countIf(rqcid != '') AS with_rqcid,
    countIf(utm_source != '') AS with_utm
FROM clickstream_events
GROUP BY event_type
ORDER BY events DESC;

-- 7. percentage of final performance events
SELECT
    countIf(event_final = 1) AS final_events,
    count() AS events,
    if(count() = 0, 0, countIf(event_final = 1) / count()) AS final_ratio
FROM clickstream_events
WHERE event_type = 'page_performance';

-- 8. invalid click ratios
SELECT count() AS invalid_click_ratio
FROM clickstream_events
WHERE event_type = 'click'
  AND (
      (click_viewport_x_ratio != -1 AND (click_viewport_x_ratio < 0 OR click_viewport_x_ratio > 1))
      OR (click_viewport_y_ratio != -1 AND (click_viewport_y_ratio < 0 OR click_viewport_y_ratio > 1))
      OR (click_document_x_ratio != -1 AND (click_document_x_ratio < 0 OR click_document_x_ratio > 1))
      OR (click_document_y_ratio != -1 AND (click_document_y_ratio < 0 OR click_document_y_ratio > 1))
  );

-- 9. invalid scroll percentage
SELECT count() AS invalid_scroll_depth
FROM clickstream_events
WHERE event_type = 'scroll'
  AND scroll_depth != -1
  AND (scroll_depth < 0 OR scroll_depth > 100);

-- 10. delivery retry distribution
SELECT delivery_transport, delivery_attempt, count() AS events
FROM clickstream_events
GROUP BY delivery_transport, delivery_attempt
ORDER BY events DESC;

-- 11. send delay distribution
SELECT
    quantiles(0.5, 0.95, 0.99)(captured_ts - sent_ts) AS send_delay_ms
FROM clickstream_events
WHERE captured_ts > 0 AND sent_ts > 0;

-- 12. web vital availability
SELECT
    countIf(web_vital_cls != -1) AS cls_present,
    countIf(web_vital_lcp != -1) AS lcp_present,
    countIf(performance_time_origin_ts != 0) AS time_origin_present,
    count() AS performance_events
FROM clickstream_events
WHERE event_type = 'page_performance';

-- 13. site_id completeness
SELECT
    countIf(site_id != 0) AS with_site_id,
    count() AS events
FROM clickstream_events;

-- timestamp order: captured_ts and occurred_ts must be positive.
-- occurred_ts > captured_ts is not an error; client clocks can run ahead.
SELECT count() AS invalid_timestamp_order
FROM clickstream_events
WHERE captured_ts <= 0 OR occurred_ts <= 0;
