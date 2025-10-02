# RCA Assignment PVH - Real-time Customer Data & Activation

## Overview
This repository demonstrates a real-time streaming pipeline for customer order events, aggregation of order data, and activation through Google Ads conversion uploads. The solution is implemented in Python and can run fully locally in mock mode for demo without GCP credentials.

---

## Repository Structure
```
rca-assignment/
├── README.md
├── requirements.txt
├── main.py                  # Entry point for streaming consumer
├── config.py                # Configurations (used only if connecting to GCP)
├── streaming/
│   ├── __init__.py
│   ├── consumer.py          # Pub/Sub subscriber and BigQuery insertion
│   └── transformer.py       # Transform raw events into BigQuery schema
├── bq/
│   ├── __init__.py
│   └── schema.sql           # BigQuery table definition for order_events
├── aggregation/
│   ├── __init__.py
│   ├── consolidate.sql      # SQL for aggregated orders table
│   └── etl.py               # Python ETL to consolidate orders table
├── activation/
│   ├── __init__.py
│   ├── google_ads_upload.py # Upload completed orders to Google Ads (mock)
│   └── required_fields.md   # Required fields for Google Ads conversion
├── diagrams/                # Architecture diagrams
├── tests/                   # Unit tests for transformer, consumer, ETL, and activation
```

---

## Setup

1. **Create and activate virtual environment**
```bash
python3 -m venv venv
source venv/bin/activate
```

2. **Install dependencies**
```bash
pip install --upgrade pip
pip install -r requirements.txt
```

3. **Configure project**
> **Note:** For this assignment and demo/interview purposes, the pipeline runs entirely in **mock mode**. You do not need to provide Google Cloud credentials, project IDs, or datasets. All events, transformations, aggregations, and Ads uploads are simulated locally.

---

## Running Streaming Consumer

- **Mock mode for local demo**
```bash
python main.py --mock --events 10 --fail-rate 0.1
```

### Mock Mode Features
- PVH-style order IDs: `pvh_amsterdam_01`, `pvh_amsterdam_02`, etc.
- Dates in `dd/mm/yyyy` format.
- Multiple events per order to demonstrate aggregation.
- Edge cases: missing fields, invalid timestamps, negative amounts.
- Color-coded console output:
  - Green → valid transformed events
  - Red → Dead Letter Queue (DLQ) events
- Tabulated display of transformed events, aggregated orders, and DLQ using `tabulate`.
- Metrics summary at the end:
```
Stream → Transform → Aggregate → Ads Upload
Total events: 10, Transformed: 8, Aggregated: 7, DLQ: 2
```
- Optional CLI arguments:
  - `--events <int>` → number of mock events to generate
  - `--fail-rate <float>` → probability of introducing invalid or missing fields

### Stress Testing
- You can simulate higher loads or failure rates to see how the application handles increased errors:
```bash
python main.py --mock --events 100 --fail-rate 0.2 --timeline --status-metrics
```
- `--events` controls the number of events generated.
- `--fail-rate` controls the probability of invalid/missing fields or edge-case events.
- This allows testing the DLQ, aggregation, and Ads upload under stress.
- Metrics and per-status counts help evaluate performance and resilience.

> **Disclaimer:** Some inconsistencies in mock runs—such as slightly misaligned failure rates or the `created_ts` format—are due to simulated/mock data. These do not reflect issues in the pipeline logic itself. I’ve chosen to share the assignment as-is to focus on the core functionality and processing flow.

---

## Aggregation

- **SQL Scheduled Query**
Run `aggregation/consolidate.sql` to create/update the consolidated `orders` table with latest status per order, partitioned by `created_ts` and clustered by `order_id`. (Used only if connecting to GCP; in mock mode, aggregation is simulated locally.)

- **Python ETL**
```bash
python aggregation/etl.py
```
- Queries `order_events` table, aggregates events per order, and writes consolidated results to the `orders` table.
- Can be scheduled hourly or triggered manually.
- Handles multiple events per order, ensuring latest status is always retained.

---

## Google Ads Activation (Mock)

```bash
python activation/google_ads_upload.py
```
- Reads completed orders from the `orders` table.
- Prepares conversion payload.
- Mocks uploading conversions to Google Ads.
- Handles edge cases: missing `gclid`, invalid currency, negative conversion values.
- Logs success vs failure.
- Required fields documented in `activation/required_fields.md`.

---

## Running Tests & Coverage

```bash
pytest -v tests/
pytest --cov=streaming --cov=aggregation --cov=activation --cov-report=term-missing
```

Tests cover:
- Event transformation (`transformer.py`)
- Streaming consumer logic (`consumer.py`)
- ETL and aggregation (`etl.py`)
- Google Ads activation (`google_ads_upload.py`)

Tests validate edge cases:
- Missing fields, invalid timestamps, negative amounts
- DLQ handling
- Multiple events per order (deduplication)

---

## Diagrams

- `diagrams/streaming.png` → Pub/Sub → Transformer → BigQuery
- `diagrams/aggregation.png` → `order_events` → Consolidation → `orders`
- `diagrams/activation.png` → `orders` → Google Ads conversion upload