from google.cloud import bigquery
import os
import logging
from datetime import datetime, timezone

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# Google Ads API mock placeholder
def upload_conversion(conversion_payload):
    """
    Mock function to simulate Google Ads conversion upload.
    Handles edge cases and logs results.
    """
    try:
        # Edge case checks
        if not conversion_payload.get("gclid"):
            raise ValueError("Missing gclid")
        if conversion_payload.get("currency_code") not in ["USD", "EUR", "GBP"]:
            raise ValueError(f"Invalid currency: {conversion_payload.get('currency_code')}")
        if conversion_payload.get("conversion_value", 0) < 0:
            raise ValueError("Negative conversion value")
        
        # Simulate upload
        logger.info(f"Successfully uploaded conversion for order_id={conversion_payload['order_id']}")
        return True
    except Exception as e:
        logger.error(f"Failed to upload conversion for order_id={conversion_payload.get('order_id')}: {e}")
        return False

# Config
PROJECT_ID = os.environ.get("PROJECT_ID", "your-gcp-project")
DATASET = os.environ.get("DATASET", "analytics")
ORDERS_TABLE = os.environ.get("ORDERS_TABLE", "orders")

REQUIRED_FIELDS = [
    "gclid",
    "conversion_action",
    "conversion_date_time",
    "conversion_value",
    "currency_code",
]

def get_completed_orders():
    client = bigquery.Client(project=PROJECT_ID)
    query = f"""
    SELECT *
    FROM `{PROJECT_ID}.{DATASET}.{ORDERS_TABLE}`
    WHERE status = 'COMPLETED'
    """
    query_job = client.query(query)
    return query_job.result()

def prepare_conversion_payload(order):
    """
    Prepare the payload required by Google Ads API.
    For demo purposes, using mock values.
    """
    
    # Convert event_ts (which is a datetime object) to an ISO string
    event_ts = getattr(order, "event_ts", "2025-10-01T12:00:00Z")
    if isinstance(event_ts, datetime):
        event_ts_str = event_ts.isoformat()
    else:
        # Fallback for string or default values
        event_ts_str = str(event_ts)
        
    payload = {
        "order_id": order.order_id,
        "gclid": getattr(order, "gclid", "TEST_GCLID"),
        "conversion_action": "ORDER_COMPLETED",
        "conversion_date_time": event_ts_str,
        "conversion_value": getattr(order, "amount", 0),
        "currency_code": getattr(order, "currency_code", "USD"),
    }
    return payload

def batch_upload(orders):
    """
    Upload conversions in batch.
    """
    success_count = 0
    fail_count = 0
    for order in orders:
        payload = prepare_conversion_payload(order)
        if upload_conversion(payload):
            success_count += 1
        else:
            fail_count += 1
    logger.info(f"Batch upload finished. Success: {success_count}, Failures: {fail_count}")

def main():
    completed_orders = get_completed_orders()
    batch_upload(completed_orders)

if __name__ == "__main__":
    main()