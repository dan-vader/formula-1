"""Message contract between the local replay producer and the bronze streaming table."""
from datetime import datetime, timezone

STREAM_ENDPOINTS = ("laps", "pit", "race_control")
EVENT_TIME_FIELDS = {"laps": "date_start", "pit": "date", "race_control": "date"}

_EARLIEST = datetime.min.replace(tzinfo=timezone.utc)


def parse_time(value):
    if not value:
        return None
    parsed = datetime.fromisoformat(value)
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def build_message(replay_id, endpoint, session_key, record):
    if endpoint not in EVENT_TIME_FIELDS:
        raise ValueError(f"Unsupported endpoint: {endpoint}")
    return {
        "replay_id": replay_id,
        "endpoint": endpoint,
        "session_key": session_key,
        "event_time": record.get(EVENT_TIME_FIELDS[endpoint]),
        "payload": record,
    }


def replay_events(records_by_endpoint, replay_id, session_key):
    """(event time, message) pairs of all endpoints in one time order; records without a time come first."""
    events = []
    for endpoint, records in records_by_endpoint.items():
        for record in records:
            message = build_message(replay_id, endpoint, session_key, record)
            events.append((parse_time(message["event_time"]), message))
    return sorted(events, key=lambda event: event[0] or _EARLIEST)


def delays(events, speed, max_sleep_sec):
    """Seconds to wait before each event: the real gap divided by the speed, capped."""
    if speed <= 0:
        raise ValueError("speed must be positive")
    previous = None
    result = []
    for when, _ in events:
        delay = 0.0
        if when is not None and previous is not None:
            delay = min(max((when - previous).total_seconds() / speed, 0.0), max_sleep_sec)
        if when is not None:
            previous = when
        result.append(delay)
    return result
