import re
from datetime import date

import pytest

from formula_1.bronze.jolpica import (
    build_url,
    iter_pages,
    landing_file,
    landing_tables,
    marker_file,
    rounds_from_schedule,
)


class FakeClient:
    def __init__(self, pages):
        self.pages = pages
        self.requested_offsets = []

    def get_json(self, url, params):
        self.requested_offsets.append(params["offset"])
        return self.pages[params["offset"]]

def page(limit, total):
    return {"MRData": {"limit": str(limit), "total": str(total)}}

def schedule(*races):
    """Jolpica schedule payload; each race is (round, date)."""
    return {
        "MRData": {
            "RaceTable": {
                "Races": [{"round": str(rnd), "date": race_date} for rnd, race_date in races]
            }
        }
    }


def test_build_url_for_pitstops_of_one_round():
    url = build_url("pitstops", season=2025, round=24)

    assert url == "https://api.jolpi.ca/ergast/f1/2025/24/pitstops.json"


def test_landing_file_path_has_parts_bronze_extracts():
    path = landing_file("/Volumes/raw", "race_results", "20251208T090000Z", 2025, "all", 100)

    assert path == (
        "/Volumes/raw/jolpica/race_results/load_id=20251208T090000Z"
        "/season=2025/round=all/offset=100.json"
    )
    assert re.search(r"load_id=([^/]+)", path).group(1) == "20251208T090000Z"
    assert re.search(r"offset=([0-9]+)[.]json", path).group(1) == "100"


def test_marker_file_lives_in_loads_folder():
    path = marker_file("/Volumes/raw", "20251208T090000Z")

    assert path == "/Volumes/raw/jolpica/_loads/load_id=20251208T090000Z/marker.json"


def test_landing_tables_map_folders_to_bronze_tables():
    tables = landing_tables()

    assert tables["race_results"] == "jolpica_race_results_raw"
    assert tables["_loads"] == "jolpica_loads_raw"

def test_iter_pages_follows_server_limit_when_page_is_capped():
    client = FakeClient({0: page(30, 70), 30: page(30, 70), 60: page(30, 70)})

    offsets = [offset for offset, _ in iter_pages(client, "url", page_limit=100)]

    assert offsets == [0, 30, 60]
    assert client.requested_offsets == [0, 30, 60]


def test_iter_pages_stops_after_single_page_when_all_fits():
    client = FakeClient({0: page(100, 45)})

    offsets = [offset for offset, _ in iter_pages(client, "url", page_limit=100)]

    assert offsets == [0]


def test_iter_pages_fails_instead_of_looping_forever_on_zero_limit():
    client = FakeClient({0: page(0, 70)})

    with pytest.raises(ValueError):
        list(iter_pages(client, "url", page_limit=100))


def test_rounds_from_schedule_keeps_only_races_before_today():
    payload = schedule(
        (1, "2025-03-16"),
        (2, "2025-03-23"),
        (3, "2025-03-30"),
    )

    rounds = rounds_from_schedule([payload], today=date(2025, 3, 23))

    assert rounds == [1]

def test_rounds_from_schedule_merges_pages_and_sorts_rounds():
    page_one = schedule(
        (4, "2025-03-19"),
        (5, "2025-03-15"),
        (6, "2025-03-30"),
    )
    page_two = schedule(
        (1, "2025-03-16"),
        (2, "2025-03-18"),
        (3, "2025-03-30"),
    )

    rounds = rounds_from_schedule([page_one, page_two], today=date(2025, 3, 29))

    assert rounds == [1, 2, 4, 5]