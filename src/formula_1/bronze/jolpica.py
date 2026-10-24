import json
import os
from datetime import date, datetime, timezone
from enum import Enum
from typing import NamedTuple

from formula_1.logging_utils import get_logger

log = get_logger("formula_1.jolpica")

BASE_URL = "https://api.jolpi.ca/ergast/f1"
LOADS_FOLDER = "_loads"


class Scope(Enum):
    GLOBAL = "global"
    SEASON = "season"
    ROUND = "round"


class Resource(NamedTuple):
    path: str
    scope: Scope


RESOURCES = {
    "schedule":       Resource("{season}.json",                  Scope.SEASON),
    "race_results":   Resource("{season}/results.json",          Scope.SEASON),
    "sprint_results": Resource("{season}/sprint.json",           Scope.SEASON),
    "drivers":        Resource("{season}/drivers.json",          Scope.SEASON),
    "constructors":   Resource("{season}/constructors.json",     Scope.SEASON),
    "status":         Resource("status.json",                    Scope.GLOBAL),
    "pitstops":       Resource("{season}/{round}/pitstops.json", Scope.ROUND),
}


def table_name(resource):
    return f"jolpica_{resource}_raw"


def landing_tables():
    """Landing folder -> bronze table, including the load markers."""
    tables = {name: table_name(name) for name in RESOURCES}
    tables[LOADS_FOLDER] = "jolpica_loads_raw"
    return tables


def build_url(resource, **parts):
    return f"{BASE_URL}/{RESOURCES[resource].path.format(**parts)}"


def iter_pages(client, url, page_limit):
    if page_limit < 1:
        raise ValueError("page_limit must be positive")
    offset = 0
    while True:
        payload = client.get_json(url, params={"limit": page_limit, "offset": offset})
        yield offset, payload
        returned_limit = int(payload["MRData"]["limit"])  # the server may cap the page size
        if returned_limit < 1:
            raise ValueError(f"Invalid limit in response from {url}")
        offset += returned_limit
        if offset >= int(payload["MRData"]["total"]):
            return


def landing_file(root, resource, load_id, season, round_label, offset):
    return (
        f"{root}/jolpica/{resource}/load_id={load_id}"
        f"/season={season}/round={round_label}/offset={offset}.json"
    )


def marker_file(root, load_id):
    return f"{root}/jolpica/{LOADS_FOLDER}/load_id={load_id}/marker.json"


def rounds_from_schedule(payloads, today=None):
    """Rounds before today; today's race may still have incomplete results."""
    today = today or datetime.now(timezone.utc).date()
    return sorted({
        int(race["round"])
        for payload in payloads
        for race in payload["MRData"]["RaceTable"]["Races"]
        if date.fromisoformat(race["date"]) < today
    })


def _write_json(path, payload):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f)


def _save_pages(client, root, load_id, resource, season, round_label, page_limit):
    url = build_url(resource, season=season, round=round_label)
    payloads = []
    for offset, payload in iter_pages(client, url, page_limit):
        if int(payload["MRData"]["total"]) == 0:
            break
        _write_json(landing_file(root, resource, load_id, season, round_label, offset), payload)
        payloads.append(payload)
    log.info("resource=%s season=%s round=%s pages=%d", resource, season, round_label, len(payloads))
    return payloads


def fetch_snapshot(client, root, load_id, seasons, page_limit, today=None):
    for folder in landing_tables():  # Auto Loader needs every folder, even an empty one
        os.makedirs(f"{root}/jolpica/{folder}", exist_ok=True)

    for name, resource in RESOURCES.items():
        if resource.scope is Scope.GLOBAL:
            _save_pages(client, root, load_id, name, "all", "all", page_limit)

    for season in seasons:
        schedule_pages = []
        for name, resource in RESOURCES.items():
            if resource.scope is Scope.SEASON:
                pages = _save_pages(client, root, load_id, name, season, "all", page_limit)
                if name == "schedule":
                    schedule_pages = pages
        for rnd in rounds_from_schedule(schedule_pages, today):
            for name, resource in RESOURCES.items():
                if resource.scope is Scope.ROUND:
                    _save_pages(client, root, load_id, name, season, rnd, page_limit)


def write_marker(root, load_id, seasons, started_at):
    """Written last: a load without a marker is incomplete and silver must ignore it."""
    _write_json(marker_file(root, load_id), {
        "load_id": load_id,
        "seasons": list(seasons),
        "started_at": started_at.isoformat(),
        "finished_at": datetime.now(timezone.utc).isoformat(),
    })
