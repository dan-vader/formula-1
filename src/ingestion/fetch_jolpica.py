import argparse
from datetime import datetime, timezone

from databricks.sdk.runtime import dbutils

from formula_1.bronze import jolpica
from formula_1.http_client import PacedClient
from formula_1.logging_utils import get_logger

log = get_logger("formula_1.fetch_jolpica")


def parse_args():
    parser = argparse.ArgumentParser(description="Download Jolpica API pages into the landing volume")
    for name in ("landing_path", "secret_scope", "contact_email_key", "seasons"):
        parser.add_argument(f"--{name.replace('_', '-')}", dest=name, required=True)
    parser.add_argument("--page-limit", type=int, required=True)
    parser.add_argument("--min-interval-sec", type=float, required=True)
    parser.add_argument("--max-requests-per-hour", type=int, required=True)
    return parser.parse_args()


def main():
    args = parse_args()
    started_at = datetime.now(timezone.utc)
    load_id = started_at.strftime("%Y%m%dT%H%M%SZ")
    seasons = list(dict.fromkeys(int(s) for s in args.seasons.split(",") if s.strip()))
    if not seasons:
        raise ValueError("--seasons must contain at least one season")
    contact_email = dbutils.secrets.get(args.secret_scope, args.contact_email_key)

    client = PacedClient(
        user_agent=f"formula-1-lakehouse (contact: {contact_email})",
        min_interval_sec=args.min_interval_sec,
        max_requests_per_hour=args.max_requests_per_hour,
    )

    log.info("Starting snapshot load_id=%s seasons=%s", load_id, seasons)
    jolpica.fetch_snapshot(client, args.landing_path, load_id, seasons, args.page_limit)
    jolpica.write_marker(args.landing_path, load_id, seasons, started_at)
    log.info("Snapshot complete load_id=%s", load_id)


if __name__ == "__main__":
    main()
