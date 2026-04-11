import re


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
