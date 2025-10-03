from google.cloud import bigquery
import os
import logging
import json
from datetime import datetime, timezone

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# Load config from environment or defaults
PROJECT_ID = os.environ.get("PROJECT_ID", "pvh-gcp-project")
DATASET = os.environ.get("DATASET", "analytics")
ORDER_EVENTS_TABLE = os.environ.get("ORDER_EVENTS_TABLE", "order_events")
ORDERS_TABLE = os.environ.get("ORDERS_TABLE", "orders")
DLQ_TABLE = os.environ.get("DLQ_TABLE", "order_events_dlq")

VALID_STATUSES = ["CREATED", "COMPLETED", "CANCELLED", "FAILED"]

def run_consolidation():
    client = bigquery.Client(project=PROJECT_ID)

    # Insert invalid events into DLQ
    dlq_query = f"""
    CREATE OR REPLACE TABLE `{PROJECT_ID}.{DATASET}.{DLQ_TABLE}` AS
    WITH invalid_events AS (
        SELECT
            TO_JSON_STRING(t) AS event,
            'Validation failed' AS error,
            CURRENT_TIMESTAMP() AS created_at
        FROM `{PROJECT_ID}.{DATASET}.{ORDER_EVENTS_TABLE}` t
        WHERE
            amount IS NULL OR amount < 0
            OR (status IS NULL OR status NOT IN UNNEST({VALID_STATUSES}))
            OR SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%S%Ez', event_ts) IS NULL
            OR event_ts IS NULL OR event_ts = ''
    )
    SELECT * FROM invalid_events
    """
    logger.info("Filtering invalid events into DLQ...")
    dlq_job = client.query(dlq_query)
    dlq_job.result()
    logger.info(f"Invalid events filtered into DLQ. Rows affected: {dlq_job.num_dml_affected_rows}")

    # Consolidate valid events into orders table with normalization
    query = f"""
    CREATE OR REPLACE TABLE `{PROJECT_ID}.{DATASET}.{ORDERS_TABLE}`
    PARTITION BY DATE(TIMESTAMP(created_ts))
    CLUSTER BY order_id
    AS
    SELECT
        order_id,
        ARRAY_AGG(STRUCT(
            IFNULL(status, 'UNKNOWN') AS status,
            amount,
            event_ts,
            IF(created_ts IS NULL OR created_ts = '', FORMAT_TIMESTAMP('%Y-%m-%dT%H:%M:%S%Ez', CURRENT_TIMESTAMP()), created_ts) AS created_ts
        )
        ORDER BY SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%S%Ez', event_ts) DESC LIMIT 1)[OFFSET(0)].*  -- latest event per order
    FROM
        `{PROJECT_ID}.{DATASET}.{ORDER_EVENTS_TABLE}`
    WHERE
        amount IS NOT NULL AND amount >= 0
        AND status IS NOT NULL AND status IN UNNEST({VALID_STATUSES})
        AND SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%S%Ez', event_ts) IS NOT NULL
        AND event_ts IS NOT NULL AND event_ts != ''
    GROUP BY
        order_id
    """
    logger.info("Starting consolidation query for valid events...")
    query_job = client.query(query)
    query_job.result()
    logger.info(f"Consolidation finished successfully. Rows affected: {query_job.num_dml_affected_rows}")

if __name__ == "__main__":
    run_consolidation()