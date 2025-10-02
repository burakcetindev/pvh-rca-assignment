import pytest
import pandas as pd
from aggregation import etl
from unittest.mock import patch, MagicMock

# Sample data simulating order_events
sample_events = [
    {"order_id": "order1", "status": "CREATED", "amount": 10, "event_ts": "2025-10-01T12:00:00Z", "created_ts": "2025-10-01T11:59:00Z"},
    {"order_id": "order1", "status": "COMPLETED", "amount": 10, "event_ts": "2025-10-01T13:00:00Z", "created_ts": "2025-10-01T11:59:00Z"},
    {"order_id": "order2", "status": "CREATED", "amount": 20, "event_ts": "2025-10-01T14:00:00Z", "created_ts": "2025-10-01T13:59:00Z"},
    {"order_id": "order2", "status": "FAILED", "amount": 20, "event_ts": "INVALID_TS", "created_ts": "2025-10-01T13:59:00Z"},
    {"order_id": "order3"} 
]

def mock_query_result(events):
    """
    Converts sample_events to objects with attribute access
    Adds invalid_count attribute defaulting to 0 if not present
    """
    class MockRow:
        def __init__(self, d):
            for k, v in d.items():
                setattr(self, k, v)
            if not hasattr(self, 'invalid_count'):
                self.invalid_count = 0
    return [MockRow(e) for e in events]

@patch("aggregation.etl.getattr")
@patch("aggregation.etl.bigquery.Client")
def test_run_consolidation(mock_client_cls, mock_getattr):
    # Patch client.query to simulate query execution
    mock_client = MagicMock()
    mock_client.query.return_value.result.return_value = mock_query_result(sample_events)
    mock_client.query.return_value.num_dml_affected_rows = 3
    mock_client_cls.return_value = mock_client

    # Patch logging to suppress output
    with patch("aggregation.etl.logger") as mock_logger:
        etl.run_consolidation()
        mock_logger.info.assert_any_call("Starting consolidation query for valid events...")
        mock_logger.info.assert_any_call("Consolidation finished successfully. Rows affected: 3")