-- Consolidated orders table: latest status per order
CREATE OR REPLACE TABLE `your-gcp-project.analytics.orders`
PARTITION BY DATE(created_ts)
CLUSTER BY order_id
AS
SELECT
    order_id,
    ARRAY_AGG(STRUCT(status, amount, event_ts, created_ts) 
              ORDER BY event_ts DESC LIMIT 1)[OFFSET(0)].*  -- latest event per order
FROM
    `your-gcp-project.analytics.order_events`
GROUP BY
    order_id;