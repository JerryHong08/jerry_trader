import re
from datetime import datetime
from zoneinfo import ZoneInfo


def _parse_number(text: str) -> float | None:
    """
    Convert string with units (M, K, B, %, etc.) to float.
    """
    if not text or text == "N/A":
        return None

    text = text.replace(",", "").strip().upper()

    match = re.match(r"([\d\.]+)\s*(M|K|B)?", text)
    if not match:
        return None

    value, unit = match.groups()
    value = float(value)

    match unit:
        case "K":
            value *= 1e3
        case "M":
            value *= 1e6
        case "B":
            value *= 1e9

    return value


def _parse_percent(text: str) -> float | None:
    if not text or text == "N/A":
        return None
    text = text.replace("%", "").strip()
    try:
        return float(text) / 100
    except:
        return None


def _parse_transfrom_timetamp(timestamp_value) -> datetime:
    # Convert timestamp to datetime
    if timestamp_value is not None:
        if isinstance(timestamp_value, (int, float)):
            # Handle numeric timestamps (assume milliseconds if > 1e10, else seconds)
            if timestamp_value > 1e10:
                timestamp = datetime.fromtimestamp(
                    timestamp_value / 1000, tz=ZoneInfo("America/New_York")
                )
            else:
                timestamp = datetime.fromtimestamp(
                    timestamp_value, tz=ZoneInfo("America/New_York")
                )
        elif isinstance(timestamp_value, str):
            # Parse string timestamp
            try:
                # Try ISO format first
                timestamp = datetime.fromisoformat(
                    timestamp_value.replace("Z", "+00:00")
                )
                if timestamp.tzinfo is None:
                    timestamp = timestamp.replace(tzinfo=ZoneInfo("America/New_York"))
                else:
                    timestamp = timestamp.astimezone(ZoneInfo("America/New_York"))
            except ValueError:
                # Try parsing as timestamp
                timestamp_float = float(timestamp_value)
                if timestamp_float > 1e10:
                    timestamp = datetime.fromtimestamp(
                        timestamp_float / 1000, tz=ZoneInfo("America/New_York")
                    )
                else:
                    timestamp = datetime.fromtimestamp(
                        timestamp_float, tz=ZoneInfo("America/New_York")
                    )
        elif isinstance(timestamp_value, datetime):
            if timestamp_value.tzinfo is None:
                timestamp = timestamp_value.replace(tzinfo=ZoneInfo("America/New_York"))
            else:
                timestamp = timestamp_value.astimezone(ZoneInfo("America/New_York"))
        else:
            print(f"WARNING: Unsupported timestamp type: {type(timestamp_value)}")
            timestamp = datetime.now(ZoneInfo("America/New_York"))

        # print(f"DEBUG: Final timestamp: {timestamp}")
    else:
        print(
            "WARNING: No timestamp found, using current time. \nNeed to Check carefully."
        )
        timestamp = datetime.now(ZoneInfo("America/New_York"))

    return timestamp
