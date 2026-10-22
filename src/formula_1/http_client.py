import time
from collections import deque

import requests


class PacedClient:
    """HTTP client with a minimum interval, a rolling hourly budget and retries."""

    def __init__(self, user_agent, min_interval_sec, max_requests_per_hour,
                 timeout_sec=30, max_retries=3, backoff_base_sec=2.0,
                 session=None, clock=time.monotonic, sleep=time.sleep):
        self._session = session or requests.Session()
        self._session.headers["User-Agent"] = user_agent
        self._min_interval = min_interval_sec
        self._max_per_hour = max_requests_per_hour
        self._timeout = timeout_sec
        self._max_retries = max_retries
        self._backoff = backoff_base_sec
        self._clock = clock
        self._sleep = sleep
        self._sent = deque()

    def _prune(self, now):
        while self._sent and now - self._sent[0] >= 3600:
            self._sent.popleft()

    def _wait_for_slot(self):
        now = self._clock()
        self._prune(now)
        if len(self._sent) >= self._max_per_hour:
            self._sleep(3600 - (now - self._sent[0]))
            now = self._clock()
            self._prune(now)
        if self._sent:
            gap = self._min_interval - (now - self._sent[-1])
            if gap > 0:
                self._sleep(gap)
        self._sent.append(self._clock())

    def get_json(self, url, params=None):
        last_error = None
        for attempt in range(self._max_retries + 1):
            self._wait_for_slot()
            try:
                response = self._session.get(url, params=params, timeout=self._timeout)
            except requests.RequestException as exc:
                last_error = exc
            else:
                if response.status_code == 429 or response.status_code >= 500:
                    last_error = RuntimeError(f"HTTP {response.status_code} for {url}")
                else:
                    response.raise_for_status()  # other 4xx (e.g. 401) fail fast
                    return response.json()
            self._sleep(self._backoff * 2 ** attempt)
        raise RuntimeError(f"Giving up on {url}") from last_error
