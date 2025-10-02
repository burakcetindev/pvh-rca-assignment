import os

# GCP Project and dataset
PROJECT_ID = os.getenv("GCP_PROJECT_ID", "your-gcp-project")
DATASET = os.getenv("BQ_DATASET", "analytics")

# BigQuery tables
ORDER_EVENTS_TABLE = os.getenv("ORDER_EVENTS_TABLE", "order_events")
CONSOLIDATED_ORDERS_TABLE = os.getenv("CONSOLIDATED_ORDERS_TABLE", "orders")

# Pub/Sub subscription
PUBSUB_SUBSCRIPTION = os.getenv("PUBSUB_SUBSCRIPTION", "orders-subscription")

# Google Ads settings
GOOGLE_ADS_CONVERSION_ACTION = os.getenv("GOOGLE_ADS_CONVERSION_ACTION", "INSERT_CONVERSION_ACTION_ID_HERE")