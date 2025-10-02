from typing import Dict, Any
import datetime

def transform_order_event(raw_event: Dict[str, Any]) -> Dict[str, Any]:
    """
    Transforms a raw order event JSON into the schema expected for BigQuery.

    Args:
        raw_event (Dict[str, Any]): Raw event data from Pub/Sub.

    Returns:
        Dict[str, Any]: Transformed dictionary with proper keys and types.
    """
    try:
        if "id" not in raw_event:
            raise ValueError("Missing required field: 'id'")

        amount = float(raw_event.get("amount", 0))
        if amount < 0:
            raise ValueError(f"Negative amount: {amount}")

        transformed = {
            "order_id": str(raw_event["id"]),
            "status": raw_event.get("status", "UNKNOWN"),
            "amount": amount,
            "event_ts": _parse_timestamp(raw_event.get("timestamp")),
            "created_ts": _parse_timestamp(raw_event.get("created_at")),
        }
        return transformed
    except KeyError as e:
        raise ValueError(f"Missing required field: {e}")
    except Exception as e:
        raise ValueError(f"Error transforming event: {e}")

def _parse_timestamp(ts: Any) -> str:
    """
    Ensures the timestamp is ISO8601 string in UTC for BigQuery.

    Supports:
    - datetime objects (assumed UTC if naive)
    - ISO8601 strings
    - Epoch seconds (int/float)
    - dd/mm/yyyy formatted strings

    Args:
        ts (Any): Input timestamp (string, int, or datetime).

    Returns:
        str: ISO8601 formatted string in UTC.
    """
    if ts is None:
        return None

    if isinstance(ts, datetime.datetime):
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=datetime.timezone.utc)
        return ts.astimezone(datetime.timezone.utc).isoformat().replace('+00:00', 'Z')

    elif isinstance(ts, str):
        # Try ISO8601 parsing first
        try:
            parsed = datetime.datetime.fromisoformat(ts.replace("Z", "+00:00"))
            return parsed.astimezone(datetime.timezone.utc).isoformat().replace('+00:00', 'Z')
        except ValueError:
            pass
        # Try dd/mm/yyyy parsing
        try:
            parsed = datetime.datetime.strptime(ts, "%d/%m/%Y")
            parsed = parsed.replace(tzinfo=datetime.timezone.utc)
            return parsed.isoformat().replace('+00:00', 'Z')
        except ValueError:
            return ts  # Return original if unparseable

    elif isinstance(ts, (int, float)):
        return datetime.datetime.fromtimestamp(ts, datetime.timezone.utc).isoformat().replace('+00:00', 'Z')

    else:
        raise ValueError(f"Cannot parse timestamp: {ts}")
