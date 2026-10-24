import argparse
from datetime import datetime, timezone

from databricks.sdk.runtime import dbutils

from formula_1.bronze import openf1
from formula_1.http_client import PacedClient
from formula_1.logging_utils import get_logger

log = get_logger("formula_1.fetch_openf1")


def parse_args():
    parser = argparse.ArgumentParser(description="Download OpenF1 API data into the landing volume")
    for name in ("landing_path", "secret_scope", "contact_email_key", "years", "session_names"):
        parser.add_argument(f"--{name.replace('_', '-')}", dest=name, required=True)
    parser.add_argument("--min-interval-sec", type=float, required=True)
    parser.add_argument("--max-requests-per-hour", type=int, required=True)
    return parser.parse_args()


def split_csv(value):
    """Comma-separated values without blanks and duplicates, order kept."""
    return list(dict.fromkeys(item.strip() for item in value.split(",") if item.strip()))


def main():
    args = parse_args()
    started_at = datetime.now(timezone.utc)
    load_id = started_at.strftime("%Y%m%dT%H%M%SZ")
    years = [int(y) for y in split_csv(args.years)]
    session_names = split_csv(args.session_names)
    if not years or not session_names:
        raise ValueError("--years and --session-names must not be empty")
    contact_email = dbutils.secrets.get(args.secret_scope, args.contact_email_key)

    client = PacedClient(
        user_agent=f"formula-1-lakehouse (contact: {contact_email})",
        min_interval_sec=args.min_interval_sec,
        max_requests_per_hour=args.max_requests_per_hour,
    )

    log.info("Starting snapshot load_id=%s years=%s sessions=%s", load_id, years, session_names)
    counts = openf1.fetch_snapshot(client, args.landing_path, load_id, years, session_names)
    openf1.write_marker(args.landing_path, load_id, years, session_names, started_at, counts)
    log.info("Snapshot complete load_id=%s", load_id)


if __name__ == "__main__":
    main()
