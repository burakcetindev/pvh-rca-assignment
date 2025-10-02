import pytest
from unittest.mock import patch, MagicMock
from activation import google_ads_upload as ga

class DummyOrder:
    def __init__(self, order_id, status, amount, event_ts):
        self.order_id = order_id
        self.status = status
        self.amount = amount
        self.event_ts = event_ts

def test_prepare_conversion_payload():
    from datetime import datetime, timezone
    order = DummyOrder("order123", "COMPLETED", 50.0, datetime(2025, 10, 1, 12, 0, tzinfo=timezone.utc))
    payload = ga.prepare_conversion_payload(order)
    assert payload["order_id"] == "order123"
    assert payload["conversion_action"] == "ORDER_COMPLETED"
    assert payload["conversion_value"] == 50.0
    assert payload["currency_code"] == "USD"
    assert payload["conversion_date_time"] == "2025-10-01T12:00:00+00:00" 

@patch("activation.google_ads_upload.get_completed_orders")
@patch("activation.google_ads_upload.upload_conversion")
def test_main_calls_upload_conversion(mock_upload, mock_get_orders):
    from datetime import datetime, timezone
    orders = [
        DummyOrder("order1", "COMPLETED", 10.0, datetime(2025, 10, 1, 12, 0, tzinfo=timezone.utc)),
        DummyOrder("order2", "COMPLETED", 20.0, datetime(2025, 10, 1, 13, 0, tzinfo=timezone.utc)),
    ]
    mock_get_orders.return_value = orders

    ga.main()

    assert mock_upload.call_count == 2
    calls_args = [call_args[0][0] for call_args in mock_upload.call_args_list]
    assert calls_args[0]["order_id"] == "order1"
    assert calls_args[1]["order_id"] == "order2"


@patch("activation.google_ads_upload.bigquery.Client")
def test_get_completed_orders_calls_query(mock_client_class):
    mock_client = MagicMock()
    mock_client_class.return_value = mock_client
    mock_query_job = MagicMock()
    mock_client.query.return_value = mock_query_job

    result = ga.get_completed_orders()
    mock_client.query.assert_called_once()
    assert result == mock_query_job.result()

def test_upload_conversion_missing_gclid(caplog):
    caplog.set_level("ERROR")
    payload = {
        "order_id": "order_missing",
        "conversion_action": "ORDER_COMPLETED",
        "conversion_date_time": "2025-10-01T12:00:00Z",
        "conversion_value": 10,
        "currency_code": "USD",
        "gclid": None
    }
    result = ga.upload_conversion(payload)
    assert result is False
    assert "Missing gclid" in caplog.text

def test_upload_conversion_invalid_currency(caplog):
    caplog.set_level("ERROR")
    payload = {
        "order_id": "order_invalid_currency",
        "conversion_action": "ORDER_COMPLETED",
        "conversion_date_time": "2025-10-01T12:00:00Z",
        "conversion_value": 10,
        "currency_code": "XYZ",
        "gclid": "TEST_GCLID"
    }
    result = ga.upload_conversion(payload)
    assert result is False
    assert "Invalid currency" in caplog.text

def test_upload_conversion_negative_value(caplog):
    caplog.set_level("ERROR")
    payload = {
        "order_id": "order_negative_value",
        "conversion_action": "ORDER_COMPLETED",
        "conversion_date_time": "2025-10-01T12:00:00Z",
        "conversion_value": -50,
        "currency_code": "USD",
        "gclid": "TEST_GCLID"
    }
    result = ga.upload_conversion(payload)
    assert result is False
    assert "Negative conversion value" in caplog.text
