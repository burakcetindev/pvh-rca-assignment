-- Consolidated orders table: latest valid status per order
CREATE OR REPLACE TABLE `your-gcp-project.analytics.orders`
PARTITION BY DATE(created_ts)
CLUSTER BY order_id
AS
SELECT
    order_id,
    ARRAY_AGG(STRUCT(status, amount, event_ts, created_ts) 
              ORDER BY event_ts DESC LIMIT 1)[OFFSET(0)].*  -- latest valid event per order
FROM
    `your-gcp-project.analytics.order_events`
WHERE
    status IS NOT NULL
    AND status != ''
    AND amount IS NOT NULL
    AND amount >= 0
    AND event_ts IS NOT NULL
    AND SAFE.PARSE_TIMESTAMP('%d/%m/%Y %H:%M:%S', event_ts) IS NOT NULL
    AND created_ts IS NOT NULL
GROUP BY
    order_id;