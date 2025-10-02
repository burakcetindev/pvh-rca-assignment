import json
import time
import logging
from google.cloud import pubsub_v1, bigquery
from streaming.transformer import transform_order_event
import config

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

MAX_RETRIES = 3

def get_bq_client():
    return bigquery.Client()

def insert_into_bigquery(row: dict):
    """
    Insert a transformed row into BigQuery table with retries and logging.
    """
    bq_client = get_bq_client()
    dataset_id = config.DATASET
    table_id = config.TABLE
    table_ref = bq_client.dataset(dataset_id).table(table_id)

    for attempt in range(1, MAX_RETRIES + 1):
        errors = bq_client.insert_rows_json(table_ref, [row])
        if not errors:
            logger.info(f"Inserted event {row['order_id']} into BigQuery")
            return
        else:
            logger.error(f"Attempt {attempt}: Error inserting {row['order_id']}: {errors}")
            if attempt < MAX_RETRIES:
                time.sleep(2 ** attempt)
            else:
                raise RuntimeError(f"Failed to insert {row['order_id']} after {MAX_RETRIES} attempts: {errors}")

def callback(message: pubsub_v1.subscriber.message.Message):
    """
    Callback function triggered for each Pub/Sub message.
    """
    try:
        raw_event = json.loads(message.data.decode("utf-8"))
        transformed = transform_order_event(raw_event)
        insert_into_bigquery(transformed)
        message.ack()
    except Exception as e:
        logger.error(f"Error processing message: {e} | Message data: {message.data}")
        message.nack()

def start_consumer():
    """
    Start the Pub/Sub subscriber to consume messages.
    """
    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(
        config.PROJECT_ID, config.PUBSUB_SUBSCRIPTION
    )
    streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
    logger.info(f"Listening for messages on {subscription_path}...")

    try:
        streaming_pull_future.result()
    except KeyboardInterrupt:
        streaming_pull_future.cancel()
        logger.info("Consumer stopped manually.")