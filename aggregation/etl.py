from google.cloud import bigquery
import os
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# Load config from environment or defaults
PROJECT_ID = os.environ.get("PROJECT_ID", "your-gcp-project")
DATASET = os.environ.get("DATASET", "analytics")
ORDER_EVENTS_TABLE = os.environ.get("ORDER_EVENTS_TABLE", "order_events")
ORDERS_TABLE = os.environ.get("ORDERS_TABLE", "orders")

def run_consolidation():
    client = bigquery.Client(project=PROJECT_ID)

    query = f"""
    CREATE OR REPLACE TABLE `{PROJECT_ID}.{DATASET}.{ORDERS_TABLE}`
    PARTITION BY DATE(created_ts)
    CLUSTER BY order_id
    AS
    SELECT
        order_id,
        ARRAY_AGG(STRUCT(status, amount, event_ts, created_ts)
                  ORDER BY event_ts DESC LIMIT 1)[OFFSET(0)].*  -- latest event per order
    FROM
        `{PROJECT_ID}.{DATASET}.{ORDER_EVENTS_TABLE}`
    GROUP BY
        order_id
    """
    logger.info("Starting consolidation query...")
    query_job = client.query(query)
    result = query_job.result()  # wait for completion
    logger.info(f"Consolidation finished successfully. Rows affected: {query_job.num_dml_affected_rows}")

if __name__ == "__main__":
    run_consolidation()