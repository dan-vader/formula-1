import json
from datetime import UTC, datetime

import pytest
import requests

from formula_1.bronze import openf1
from formula_1.bronze.openf1 import completed_session_keys, fetch_json, fetch_snapshot

NOW = datetime(2025, 12, 8, 12, 0, tzinfo=UTC)
LOAD_ID = "20251208T090000Z"


def session(key, name, date_end):
    return {"session_key": key, "session_name": name, "date_end": date_end}


class FakeResponse:
    def __init__(self, status_code, body):
        self.status_code = status_code
        self._body = body

    def json(self):
        return self._body


class FakeClient:
    """Returns a prepared answer per endpoint; an exception as the answer is raised."""

    def __init__(self, answers):
        self.answers = answers

    def get_json(self, url, params=None):
        answer = self.answers[url.rsplit("/", 1)[-1]]
        if isinstance(answer, Exception):
            raise answer
        return answer


def http_error(status_code, body):
    return requests.HTTPError(response=FakeResponse(status_code, body))


def test_completed_session_keys_keeps_only_finished_races_and_sprints():
    sessions = [
        session(9839, "Race", "2025-12-07T15:00:00+00:00"),
        session(9838, "Qualifying", "2025-12-06T15:00:00+00:00"),
        session(9900, "Race", "2025-12-14T15:00:00+00:00"),
        session(9837, "Sprint", None),
    ]

    assert completed_session_keys(sessions, ["Race", "Sprint"], now=NOW) == [9839]


def test_completed_session_keys_treats_time_without_zone_as_utc_and_sorts():
    sessions = [
        session(20, "Sprint", "2025-12-06T15:00:00"),
        session(10, "Race", "2025-12-07T15:00:00"),
    ]

    assert completed_session_keys(sessions, ["Race", "Sprint"], now=NOW) == [10, 20]


def test_fetch_json_returns_empty_list_for_no_results_404():
    client = FakeClient({"laps": http_error(404, {"detail": "No results found."})})

    assert fetch_json(client, "laps", session_key=9839) == []


def test_fetch_json_raises_for_other_404():
    client = FakeClient({"laps": http_error(404, {"detail": "Not Found"})})

    with pytest.raises(requests.HTTPError):
        fetch_json(client, "laps", session_key=9839)


def test_fetch_json_rejects_response_that_is_not_a_list():
    client = FakeClient({"laps": {"error": "something went wrong"}})

    with pytest.raises(ValueError):
        fetch_json(client, "laps", session_key=9839)


def test_fetch_snapshot_writes_files_and_counts_rows(tmp_path):
    answers = {endpoint: [] for endpoint in openf1.SESSION_ENDPOINTS}
    answers["sessions"] = [session(9839, "Race", "2025-12-07T15:00:00+00:00")]
    answers["meetings"] = [{"meeting_key": 1276}]
    answers["laps"] = [{"lap_number": 1}, {"lap_number": 2}]

    counts = fetch_snapshot(FakeClient(answers), str(tmp_path), LOAD_ID, [2025], ["Race"], now=NOW)

    assert counts["laps"] == {"files": 1, "rows": 2}
    assert counts["sessions"] == {"files": 1, "rows": 1}
    assert "pit" not in counts
    laps_file = tmp_path / f"openf1/laps/load_id={LOAD_ID}/year=2025/session_key=9839/data.json"
    assert json.loads(laps_file.read_text()) == [{"lap_number": 1}, {"lap_number": 2}]


def test_fetch_snapshot_fails_when_year_has_no_sessions(tmp_path):
    client = FakeClient({"sessions": [], "meetings": []})

    with pytest.raises(RuntimeError):
        fetch_snapshot(client, str(tmp_path), LOAD_ID, [2025], ["Race"], now=NOW)
