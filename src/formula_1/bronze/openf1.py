import json
import os
from collections import defaultdict
from datetime import datetime, timezone

import requests

from formula_1.logging_utils import get_logger

log = get_logger("formula_1.openf1")

BASE_URL = "https://api.openf1.org/v1"
LOADS_FOLDER = "_loads"
NO_SESSION = -1  # sentinel in the landing path for endpoints that are not per session
NO_RESULTS_MESSAGE = "no results found"  # `detail` of the HTTP 404 that OpenF1 sends for an empty result

YEAR_ENDPOINTS = ("sessions", "meetings")
SESSION_ENDPOINTS = ("drivers", "laps", "stints", "pit", "race_control", "overtakes", "position")


def table_name(endpoint):
    return f"openf1_{endpoint}_raw"


def landing_tables():
    """Landing folder -> bronze table, including the load markers."""
    tables = {name: table_name(name) for name in YEAR_ENDPOINTS + SESSION_ENDPOINTS}
    tables[LOADS_FOLDER] = "openf1_loads_raw"
    return tables


def landing_file(root, endpoint, load_id, year, session_key):
    return f"{root}/openf1/{endpoint}/load_id={load_id}/year={year}/session_key={session_key}/data.json"


def marker_file(root, load_id):
    return f"{root}/openf1/{LOADS_FOLDER}/load_id={load_id}/marker.json"


def _is_no_results(exc):
    """True only for the documented 'no results' 404; any other 404 is a real error."""
    response = exc.response
    if response is None or response.status_code != 404:
        return False
    try:
        detail = response.json().get("detail", "")
    except (ValueError, AttributeError):
        return False
    return NO_RESULTS_MESSAGE in str(detail).lower()


def fetch_json(client, endpoint, **params):
    """OpenF1 returns a JSON array; an empty result comes back as HTTP 404 with a 'no results' detail."""
    try:
        payload = client.get_json(f"{BASE_URL}/{endpoint}", params=params)
    except requests.HTTPError as exc:
        if _is_no_results(exc):
            return []
        raise
    if not isinstance(payload, list):
        raise ValueError(f"Unexpected response from {endpoint}: {str(payload)[:200]}")
    return payload


def completed_session_keys(sessions, session_names, now=None):
    """Keys of finished sessions with the requested names (e.g. Race, Sprint)."""
    now = now or datetime.now(timezone.utc)
    keys = set()
    for session in sessions:
        if session.get("session_name") not in session_names:
            continue
        date_end = session.get("date_end")
        if not date_end:
            log.warning("Skipping session without date_end: %s", session.get("session_key"))
            continue
        date_end = datetime.fromisoformat(date_end)
        if date_end.tzinfo is None:
            date_end = date_end.replace(tzinfo=timezone.utc)
        if date_end < now:
            keys.add(int(session["session_key"]))
    return sorted(keys)


def _write_json(path, payload):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f)


def _save(client, root, load_id, endpoint, year, session_key, query, counts):
    payload = fetch_json(client, endpoint, **query)
    log.info("endpoint=%s year=%s session_key=%s rows=%d", endpoint, year, session_key, len(payload))
    if payload:
        _write_json(landing_file(root, endpoint, load_id, year, session_key), payload)
        counts[endpoint]["files"] += 1
        counts[endpoint]["rows"] += len(payload)
    return payload


def fetch_snapshot(client, root, load_id, years, session_names, now=None):
    """Download everything and return {endpoint: {"files": n, "rows": m}} for the marker."""
    for folder in landing_tables():  # Auto Loader needs every folder, even an empty one
        os.makedirs(f"{root}/openf1/{folder}", exist_ok=True)

    counts = defaultdict(lambda: {"files": 0, "rows": 0})
    for year in years:
        sessions = []
        for endpoint in YEAR_ENDPOINTS:
            payload = _save(client, root, load_id, endpoint, year, NO_SESSION, {"year": year}, counts)
            if endpoint == "sessions":
                sessions = payload
        if not sessions:
            raise RuntimeError(f"OpenF1 returned no sessions for year {year}")
        for session_key in completed_session_keys(sessions, session_names, now):
            for endpoint in SESSION_ENDPOINTS:
                _save(client, root, load_id, endpoint, year, session_key, {"session_key": session_key}, counts)
    return dict(counts)


def write_marker(root, load_id, years, session_names, started_at, counts):
    """Written last: a load without a marker is incomplete and silver must ignore it."""
    _write_json(marker_file(root, load_id), {
        "load_id": load_id,
        "years": list(years),
        "session_names": list(session_names),
        "counts": counts,
        "started_at": started_at.isoformat(),
        "finished_at": datetime.now(timezone.utc).isoformat(),
    })
