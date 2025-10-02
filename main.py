import argparse
import random
from datetime import datetime, timezone, timedelta
from tabulate import tabulate
from colorama import Fore, Style, init
from streaming import transformer
from activation import google_ads_upload as ga

init(autoreset=True)

def generate_mock_events(num_events=10, fail_rate=0.1):
    events = []
    base_date = datetime(2025, 9, 1, 12, 0, tzinfo=timezone.utc)
    for i in range(1, num_events + 1):
        order_id = f"pvh_amsterdam_{i:02d}"
        status = random.choice(["CREATED", "COMPLETED", "CANCELLED", "FAILED"])
        amount = random.randint(0, 50)
        created_ts = (base_date + timedelta(hours=i)).strftime("%d/%m/%Y %H:%M:%S")
        # Introduce failures randomly
        if random.random() < fail_rate:
            timestamp = "INVALID_TIMESTAMP"
        else:
            timestamp = (base_date + timedelta(hours=i+1)).strftime("%d/%m/%Y %H:%M:%S")
        # Randomly remove status or amount to simulate missing fields
        if random.random() < fail_rate:
            status = None
        if random.random() < fail_rate:
            amount = None
        event = {
            "id": order_id,
            "status": status,
            "amount": amount,
            "timestamp": timestamp,
            "created_at": created_ts
        }
        events.append(event)

    # Add extra DLQ-triggering events
    # Event with missing timestamp
    events.append({
        "id": f"pvh_amsterdam_{num_events + 1:02d}",
        "status": "COMPLETED",
        "amount": 25,
        # missing timestamp key
        "created_at": (base_date + timedelta(hours=num_events + 1)).strftime("%d/%m/%Y %H:%M:%S")
    })
    # Event with missing created_at
    events.append({
        "id": f"pvh_amsterdam_{num_events + 2:02d}",
        "status": "CREATED",
        "amount": 30,
        "timestamp": (base_date + timedelta(hours=num_events + 2)).strftime("%d/%m/%Y %H:%M:%S"),
        # missing created_at key
    })
    # Event with invalid timestamp string
    events.append({
        "id": f"pvh_amsterdam_{num_events + 3:02d}",
        "status": "FAILED",
        "amount": 15,
        "timestamp": "2025/99/99 99:99:99",
        "created_at": (base_date + timedelta(hours=num_events + 3)).strftime("%d/%m/%Y %H:%M:%S")
    })
    # Event with negative amount
    events.append({
        "id": f"pvh_amsterdam_{num_events + 4:02d}",
        "status": "COMPLETED",
        "amount": -10,
        "timestamp": (base_date + timedelta(hours=num_events + 4)).strftime("%d/%m/%Y %H:%M:%S"),
        "created_at": (base_date + timedelta(hours=num_events + 4)).strftime("%d/%m/%Y %H:%M:%S")
    })

    return events

class DummyOrder:
    """Wraps a dict to provide attribute-style access for prepare_conversion_payload"""
    def __init__(self, order_dict):
        self.order_id = order_dict.get("order_id")
        self.status = order_dict.get("status")
        self.amount = order_dict.get("amount")
        self.event_ts = order_dict.get("event_ts")
        self.created_ts = order_dict.get("created_ts")

def run_mock(num_events=10, fail_rate=0.1, show_timeline=False, show_status_metrics=False):
    print("Running in mock mode with PVH-style events...\n")
    mock_events = generate_mock_events(num_events, fail_rate)

    transformed_rows = []
    dlq_events = []

    print("--- Transformation Step ---")
    for event in mock_events:
        try:
            transformed = transformer.transform_order_event(event)
            transformed_rows.append(transformed)
            print(Fore.GREEN + f"Transformed: {transformed['order_id']}")
        except Exception as e:
            dlq_events.append({"event": event, "error": str(e)})
            print(Fore.RED + f"DLQ Event: {event['id'] if 'id' in event else 'UNKNOWN'} | Error: {e}")

    print("\n--- Aggregation Step ---")
    # Aggregate latest status per order
    orders = {}
    for row in transformed_rows:
        order_id = row["order_id"]
        if order_id not in orders or row["event_ts"] > orders[order_id]["event_ts"]:
            orders[order_id] = row

    # Display transformed events table
    if transformed_rows:
        print("\n--- Transformed Events Table ---")
        print(tabulate(transformed_rows, headers="keys", tablefmt="grid"))

    # Display aggregated orders table
    if orders:
        print("\n--- Aggregated Orders Table ---")
        print(tabulate(orders.values(), headers="keys", tablefmt="grid"))

    # Display DLQ table
    if dlq_events:
        print("\n--- Dead Letter Queue Table ---")
        print(tabulate(dlq_events, headers="keys", tablefmt="grid"))

    # Mock Google Ads upload
    print("\n--- Google Ads Upload Step ---")
    upload_results = {}
    for order_dict in orders.values():
        if order_dict["status"] == "COMPLETED":
            order_obj = DummyOrder(order_dict)
            payload = ga.prepare_conversion_payload(order_obj)
            success = ga.upload_conversion(payload)
            upload_results[order_dict["order_id"]] = success
            status_str = Fore.GREEN + "SUCCESS" if success else Fore.RED + "FAILED"
            print(f"Order {order_obj.order_id} -> Google Ads Upload: {status_str}")
        else:
            upload_results[order_dict["order_id"]] = None  # Not uploaded

    # Timeline visualization
    if show_timeline:
        print("\n--- Order Processing Timeline ---")
        timeline_header = ["Order ID", "Created", "Transformed", "Aggregated", "Google Ads Upload"]
        timeline_rows = []
        # Build a set of all order IDs from mock_events to include those that failed transform
        all_order_ids = set(event.get("id") for event in mock_events)
        for order_id in sorted(all_order_ids):
            # Created stage: always green (event exists)
            created_stage = Fore.GREEN + "✔" + Style.RESET_ALL
            # Transformed stage: green if transformed, red if in DLQ, else red (fail)
            transformed_stage = Fore.RED + "✘" + Style.RESET_ALL
            # Aggregated stage: green if aggregated, else red
            aggregated_stage = Fore.RED + "✘" + Style.RESET_ALL
            # Google Ads Upload stage: green if uploaded success, red if uploaded fail, yellow if aggregated but not uploaded
            ga_stage = Fore.RED + "✘" + Style.RESET_ALL

            # Check transformed
            transformed_entry = next((t for t in transformed_rows if t["order_id"] == order_id), None)
            if transformed_entry:
                transformed_stage = Fore.GREEN + "✔" + Style.RESET_ALL
            else:
                # Check if in DLQ
                if any(dlq.get("event", {}).get("id") == order_id for dlq in dlq_events):
                    transformed_stage = Fore.RED + "✘" + Style.RESET_ALL
                else:
                    transformed_stage = Fore.RED + "✘" + Style.RESET_ALL

            # Check aggregated
            if order_id in orders:
                aggregated_stage = Fore.GREEN + "✔" + Style.RESET_ALL
            else:
                aggregated_stage = Fore.RED + "✘" + Style.RESET_ALL

            # Check Google Ads Upload
            if order_id in upload_results:
                if upload_results[order_id] is True:
                    ga_stage = Fore.GREEN + "✔" + Style.RESET_ALL
                elif upload_results[order_id] is False:
                    ga_stage = Fore.RED + "✘" + Style.RESET_ALL
                else:
                    # aggregated but not uploaded
                    ga_stage = Fore.YELLOW + "●" + Style.RESET_ALL
            else:
                ga_stage = Fore.RED + "✘" + Style.RESET_ALL

            timeline_rows.append([order_id, created_stage, transformed_stage, aggregated_stage, ga_stage])

        print(tabulate(timeline_rows, headers=timeline_header, tablefmt="grid"))

    # Enhanced Metrics Summary
    print("\n--- Metrics Summary ---")
    print(f"Total Events Processed: {len(mock_events)}")
    print(f"Transformed Events: {len(transformed_rows)}")
    print(f"Aggregated Orders: {len(orders)}")
    print(f"DLQ Events: {len(dlq_events)}")
    print(f"Google Ads Uploads: {sum(1 for v in upload_results.values() if v is True)}")
    print(f"Google Ads Upload Failures: {sum(1 for v in upload_results.values() if v is False)}")
    print(f"Orders Aggregated but Not Uploaded: {sum(1 for v in upload_results.values() if v is None)}")

    if show_status_metrics:
        print("\n--- Per-Status Counts ---")
        status_counts = {}
        for event in mock_events:
            status = event.get("status") or "UNKNOWN"
            status_counts[status] = status_counts.get(status, 0) + 1
        status_table = [[status, count] for status, count in sorted(status_counts.items())]
        print(tabulate(status_table, headers=["Status", "Count"], tablefmt="grid"))

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="RCA Streaming Consumer")
    parser.add_argument("--mock", action="store_true", help="Run in mock mode with local events")
    parser.add_argument("--events", type=int, default=10, help="Number of mock events to generate")
    parser.add_argument("--fail-rate", type=float, default=0.1, help="Simulated failure rate for mock events")
    parser.add_argument("--timeline", action="store_true", help="Display order processing timeline")
    parser.add_argument("--status-metrics", action="store_true", help="Display per-status counts in metrics summary")
    args = parser.parse_args()

    if args.mock:
        run_mock(num_events=args.events, fail_rate=args.fail_rate, show_timeline=args.timeline, show_status_metrics=args.status_metrics)
    else:
        from streaming.consumer import start_consumer
        try:
            start_consumer()
        except KeyboardInterrupt:
            print("Consumer stopped manually.")