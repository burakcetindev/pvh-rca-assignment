from typing import Dict, Any
import datetime

VALID_STATUSES = {"CREATED", "COMPLETED", "FAILED", "CANCELLED"}

def transform_order_event(raw_event: Dict[str, Any]) -> Dict[str, Any]:
    """
    Transforms a raw order event JSON into the schema expected for BigQuery.
    Validates amount, status, and timestamp fields. Returns a dict with dlq_reason if invalid.
    """
    try:
        amount = raw_event.get("amount")
        # Validate amount presence and type
        if amount is None or not isinstance(amount, (int, float, str)):
            return {"dlq_reason": "Invalid amount: missing or wrong type"}
        try:
            amount = float(amount)
        except Exception:
            return {"dlq_reason": "Invalid amount: not convertible to float"}
        # Amount must be non-negative
        if amount < 0:
            return {"dlq_reason": "Invalid amount: cannot be negative"}

        status = raw_event.get("status")
        # Normalize status to uppercase string if possible
        if not isinstance(status, str) or status.upper() not in VALID_STATUSES:
            status = "UNKNOWN"
        else:
            status = status.upper()

        event_ts = _parse_timestamp(raw_event.get("timestamp"))
        # event_ts must be a valid ISO8601 timestamp string
        if event_ts is None:
            return {"dlq_reason": "Invalid timestamp: unparseable or missing"}

        created_at_raw = raw_event.get("created_at")
        created_ts = _parse_timestamp(created_at_raw)
        # If created_at is missing or unparseable, use current UTC timestamp
        if created_at_raw is None or created_ts is None:
            created_ts = datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc).isoformat().replace('+00:00', 'Z')

        transformed = {
            "order_id": str(raw_event.get("id", "")),
            "status": status,
            "amount": amount,
            "event_ts": event_ts,
            "created_ts": created_ts,
        }
        return transformed
    except Exception as e:
        return {"dlq_reason": f"Error transforming event: {e}"}

def _parse_timestamp(ts: Any) -> str:
    """
    Ensures the timestamp is ISO8601 string in UTC for BigQuery.
    Returns None if unparseable.
    """
    if ts is None:
        return None
        
    if isinstance(ts, datetime.datetime):
        # If naive datetime, assume UTC
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=datetime.timezone.utc)
        return ts.astimezone(datetime.timezone.utc).isoformat().replace('+00:00', 'Z')
        
    elif isinstance(ts, str):
        try:
            # Attempt ISO8601 parsing with timezone info
            parsed = datetime.datetime.fromisoformat(ts.replace("Z", "+00:00"))
            return parsed.astimezone(datetime.timezone.utc).isoformat().replace('+00:00', 'Z')
        except ValueError:
            pass
        try:
            # Attempt parsing common non-ISO format: "dd/mm/YYYY HH:MM:SS"
            parsed = datetime.datetime.strptime(ts, "%d/%m/%Y %H:%M:%S")
            parsed = parsed.replace(tzinfo=datetime.timezone.utc)
            return parsed.isoformat().replace('+00:00', 'Z')
        except ValueError:
            return None
            
    elif isinstance(ts, (int, float)):
        try:
            # Assume ts is a UNIX timestamp in seconds
            return datetime.datetime.fromtimestamp(ts, datetime.timezone.utc).isoformat().replace('+00:00', 'Z')
        except Exception:
            return None
    return None