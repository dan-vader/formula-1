"""Replay a finished OpenF1 session into Event Hub, simulating a live stream.

Events are sent in their original order with the real time gaps divided by
--speed (each pause is capped by --max-sleep-sec).

Setup:
    uv sync --extra producer

Secrets (either way):
    - environment variables EH_CONNECTION_STR and CONTACT_EMAIL
    - or --key-vault-url (run `az login` first); the secrets are read from
      formula-1-eventhub-send-conn-str and formula-1-contact-email

Example:
    uv run python producer/replay_openf1.py --session-key 11731 --speed 100
    uv run python producer/replay_openf1.py --session-key 11731 --dry-run
"""
import argparse
import json
import os
import time
import uuid

from formula_1.bronze import openf1, openf1_stream
from formula_1.http_client import PacedClient
from formula_1.logging_utils import get_logger

log = get_logger("formula_1.replay_openf1")

SEND_SECRET = "formula-1-eventhub-send-conn-str"
CONTACT_SECRET = "formula-1-contact-email"


def parse_args():
    parser = argparse.ArgumentParser(description="Replay an OpenF1 session into Event Hub")
    parser.add_argument("--session-key", type=int, required=True)
    parser.add_argument("--endpoints", default=",".join(openf1_stream.STREAM_ENDPOINTS))
    parser.add_argument("--speed", type=float, default=60.0, help="race seconds per real second")
    parser.add_argument("--max-sleep-sec", type=float, default=5.0, help="cap for a single pause")
    parser.add_argument("--max-events", type=int, default=0, help="0 sends everything")
    parser.add_argument("--replay-id", default=None, help="stable id of this run, generated if omitted")
    parser.add_argument("--eventhub-name", default=os.environ.get("EH_NAME"))
    parser.add_argument("--key-vault-url", default=os.environ.get("KEY_VAULT_URL"))
    parser.add_argument("--dry-run", action="store_true", help="fetch and print, send nothing")
    return parser.parse_args()


def get_secret(env_name, secret_name, key_vault_url):
    value = os.environ.get(env_name)
    if value:
        return value
    if not key_vault_url:
        raise SystemExit(f"Set {env_name} or pass --key-vault-url")
    from azure.identity import DefaultAzureCredential
    from azure.keyvault.secrets import SecretClient

    return SecretClient(vault_url=key_vault_url, credential=DefaultAzureCredential()).get_secret(secret_name).value


def fetch_records(client, endpoints, session_key):
    records = {}
    for endpoint in endpoints:
        records[endpoint] = openf1.fetch_json(client, endpoint, session_key=session_key)
        log.info("endpoint=%s session_key=%s rows=%d", endpoint, session_key, len(records[endpoint]))
    return records


def main():
    args = parse_args()
    endpoints = [e.strip() for e in args.endpoints.split(",") if e.strip()]
    unsupported = set(endpoints) - set(openf1_stream.STREAM_ENDPOINTS)
    if not endpoints or unsupported:
        raise SystemExit(f"--endpoints must be a subset of {openf1_stream.STREAM_ENDPOINTS}")
    replay_id = args.replay_id or f"{args.session_key}-{uuid.uuid4().hex[:8]}"

    contact_email = get_secret("CONTACT_EMAIL", CONTACT_SECRET, args.key_vault_url)
    client = PacedClient(
        user_agent=f"formula-1-lakehouse (contact: {contact_email})",
        min_interval_sec=2.5,
        max_requests_per_hour=1400,
    )
    records = fetch_records(client, endpoints, args.session_key)
    events = openf1_stream.replay_events(records, replay_id, args.session_key)
    if args.max_events:
        events = events[: args.max_events]
    pauses = openf1_stream.delays(events, args.speed, args.max_sleep_sec)
    log.info("replay_id=%s events=%d speed=%s", replay_id, len(events), args.speed)

    if args.dry_run:
        for _, message in events[:5]:
            print(json.dumps(message)[:300])
        return

    from azure.eventhub import EventData, EventHubProducerClient

    connection_string = get_secret("EH_CONNECTION_STR", SEND_SECRET, args.key_vault_url)
    producer = EventHubProducerClient.from_connection_string(connection_string, eventhub_name=args.eventhub_name)
    with producer:
        for number, ((_, message), pause) in enumerate(zip(events, pauses), start=1):
            time.sleep(pause)
            batch = producer.create_batch()
            batch.add(EventData(json.dumps(message)))
            producer.send_batch(batch)
            if number % 200 == 0:
                log.info("sent %d of %d events", number, len(events))
    log.info("Replay complete replay_id=%s events=%d", replay_id, len(events))


if __name__ == "__main__":
    main()
