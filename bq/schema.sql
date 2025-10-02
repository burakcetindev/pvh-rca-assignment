CREATE TABLE IF NOT EXISTS `your-gcp-project.analytics.order_events` (
    order_id STRING NOT NULL,
    status STRING,
    amount FLOAT64,
    event_ts TIMESTAMP,
    created_ts TIMESTAMP
)
PARTITION BY DATE(created_ts)
CLUSTER BY order_id;
