import json
import pytest
from unittest.mock import patch, MagicMock 
from streaming import consumer

class DummyMessage:
    def __init__(self, data):
        self.data = data
        self.acked = False
        self.nacked = False

    def ack(self):
        self.acked = True

    def nack(self):
        self.nacked = True

def test_callback_success(monkeypatch):
    # Prepare dummy Pub/Sub message
    raw_event = {
        "id": "order123",
        "status": "CREATED",
        "amount": 10.5,
        "timestamp": "2025-10-01T12:00:00Z",
        "created_at": "2025-10-01T11:59:00Z"
    }
    message = DummyMessage(data=json.dumps(raw_event).encode("utf-8"))

    # Patch insert_into_bigquery to simulate success
    monkeypatch.setattr(consumer, "insert_into_bigquery", lambda row: None)

    consumer.callback(message)

    assert message.acked is True
    assert message.nacked is False


def test_callback_failure(monkeypatch):
    # Prepare invalid Pub/Sub message
    message = DummyMessage(data=b'{"invalid": "payload"}')

    # Patch transform_order_event to raise exception
    with patch('streaming.consumer.transform_order_event', side_effect=ValueError("Transform failed")):
        consumer.callback(message)

    assert message.acked is False
    assert message.nacked is True

# Mock the config module with the required attributes for these two tests
@patch.object(consumer, 'config', MagicMock(DATASET='test_dataset', TABLE='test_table'))
def test_insert_into_bigquery_success(monkeypatch):
    # Mock the BigQuery client
    mock_client = MagicMock()
    monkeypatch.setattr(consumer, "get_bq_client", lambda: mock_client)
    mock_client.insert_rows_json.return_value = []

    consumer.insert_into_bigquery({"order_id": "1"})

    mock_client.insert_rows_json.assert_called_once()
    mock_client.dataset.assert_called_with('test_dataset')
    mock_client.dataset().table.assert_called_with('test_table')


# Mock the config module with the required attributes for these two tests
@patch.object(consumer, 'config', MagicMock(DATASET='test_dataset', TABLE='test_table'))
def test_insert_into_bigquery_failure(monkeypatch):
    # Mock the BigQuery client
    mock_client = MagicMock()
    monkeypatch.setattr(consumer, "get_bq_client", lambda: mock_client)
    # Simulate BQ returning errors
    mock_client.insert_rows_json.return_value = [{"error": "bad row"}]

    with pytest.raises(RuntimeError):
        consumer.insert_into_bigquery({"order_id": "1"})
        
    assert consumer.MAX_RETRIES == 3 # Sanity check
    mock_client.insert_rows_json.assert_called_with(mock_client.dataset().table(), [{'order_id': '1'}])
    assert mock_client.insert_rows_json.call_count == consumer.MAX_RETRIES

def test_callback_missing_fields(monkeypatch):
    # Event missing required fields (e.g. status, amount, timestamps)
    raw_event = {
        "id": "order_missing"
        # missing status, amount, timestamps
    }
    message = DummyMessage(data=json.dumps(raw_event).encode("utf-8"))
    # Patch transformer to raise ValueError on missing fields
    with patch('streaming.consumer.transform_order_event', side_effect=ValueError("Missing required field: 'status'")):
        consumer.callback(message)
    assert message.acked is False
    assert message.nacked is True


def test_callback_invalid_timestamp(monkeypatch):
    # Event with invalid timestamp format
    raw_event = {
        "id": "order_invalid_ts",
        "status": "CREATED",
        "amount": 10,
        "timestamp": "INVALID_TS",
        "created_at": "2025-10-01T12:00:00Z"
    }
    message = DummyMessage(data=json.dumps(raw_event).encode("utf-8"))
    with patch('streaming.consumer.transform_order_event', side_effect=ValueError("Cannot parse timestamp: INVALID_TS")):
        consumer.callback(message)
    assert message.acked is False
    assert message.nacked is True


def test_callback_negative_amount(monkeypatch):
    # Event with negative amount (should still be processed if not explicitly forbidden)
    raw_event = {
        "id": "order_negative",
        "status": "CREATED",
        "amount": -50,
        "timestamp": "2025-10-01T12:00:00Z",
        "created_at": "2025-10-01T11:59:00Z"
    }
    message = DummyMessage(data=json.dumps(raw_event).encode("utf-8"))
    monkeypatch.setattr(consumer, "insert_into_bigquery", lambda row: None)
    consumer.callback(message)
    # Assuming transformer allows negative amounts, this should ack
    assert message.acked is True
    assert message.nacked is False


def test_callback_multiple_events_dlq(monkeypatch):
    # Multiple events, some invalid, to check DLQ handling
    events = [
        {"id": "order1", "amount": 10, "timestamp": "INVALID_TS", "created_at": "2025-10-01T12:00:00Z"},
        {"id": "order2", "amount": 20, "timestamp": "2025-10-01T13:00:00Z", "created_at": "2025-10-01T12:59:00Z"},
        {"id": "order3"}  # missing fields
    ]
    messages = [DummyMessage(data=json.dumps(e).encode("utf-8")) for e in events]
    
    monkeypatch.setattr(consumer, "insert_into_bigquery", lambda row: None)

    for msg in messages:
        # Use actual callback but patch transformer to handle invalid/missing events
        def side_effect(event):
            if event["id"] == "order1":
                raise ValueError("Cannot parse timestamp")
            elif event["id"] == "order3":
                raise ValueError("Missing required field: 'status'")
            else:
                return {"order_id": event["id"], "status": "CREATED", "amount": event.get("amount", 0),
                        "event_ts": "2025-10-01T12:00:00Z", "created_ts": "2025-10-01T11:59:00Z"}

        with patch('streaming.consumer.transform_order_event', side_effect=side_effect):
            consumer.callback(msg)

    assert messages[0].acked is False
    assert messages[0].nacked is True
    assert messages[1].acked is True
    assert messages[1].nacked is False
    assert messages[2].acked is False
    assert messages[2].nacked is True