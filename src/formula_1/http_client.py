import time
from collections import deque

import requests
from tenacity import Retrying, retry_if_exception, stop_after_attempt, wait_exponential

from formula_1.logging_utils import get_logger

log = get_logger("formula_1.http")


class RetryableHttpError(Exception):
    """HTTP 429 or 5xx response."""

    def __init__(self, status_code, url, retry_after=None):
        super().__init__(f"HTTP {status_code} for {url}")
        self.retry_after = retry_after


def _is_retryable(exc):
    return isinstance(exc, (requests.ConnectionError, requests.Timeout, RetryableHttpError))


def _retry_after(response):
    try:
        return float(response.headers.get("Retry-After"))
    except (TypeError, ValueError):
        return None


class PacedClient:
    """HTTP client with a minimum interval, a rolling hourly budget and retries.

    Every attempt, retries included, waits for a slot and counts against the budget.
    """

    def __init__(self, user_agent, min_interval_sec, max_requests_per_hour,
                 timeout_sec=30, max_attempts=4, backoff_base_sec=2.0, backoff_max_sec=60.0,
                 session=None, clock=time.monotonic, sleep=time.sleep):
        if max_requests_per_hour < 1:
            raise ValueError("max_requests_per_hour must be positive")
        self._headers = {"User-Agent": user_agent}
        self._session = session or requests.Session()
        self._min_interval = min_interval_sec
        self._max_per_hour = max_requests_per_hour
        self._timeout = timeout_sec
        self._max_attempts = max_attempts
        self._backoff = wait_exponential(multiplier=backoff_base_sec, max=backoff_max_sec)
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
            wait = 3600 - (now - self._sent[0])
            log.warning("Hourly request budget reached, waiting %.0f s", wait)
            self._sleep(wait)
            now = self._clock()
            self._prune(now)
        if self._sent:
            gap = self._min_interval - (now - self._sent[-1])
            if gap > 0:
                self._sleep(gap)
        self._sent.append(self._clock())

    def _wait_before_retry(self, retry_state):
        retry_after = getattr(retry_state.outcome.exception(), "retry_after", None)
        if retry_after is not None:
            return retry_after
        return self._backoff(retry_state)

    def _get_once(self, url, params):
        self._wait_for_slot()
        response = self._session.get(
            url, params=params, headers=self._headers, timeout=self._timeout
        )
        if response.status_code == 429 or response.status_code >= 500:
            raise RetryableHttpError(response.status_code, url, _retry_after(response))
        response.raise_for_status()  # other 4xx (e.g. 401) fail fast
        return response.json()

    def get_json(self, url, params=None):
        retrying = Retrying(
            retry=retry_if_exception(_is_retryable),
            stop=stop_after_attempt(self._max_attempts),
            wait=self._wait_before_retry,
            sleep=self._sleep,
            before_sleep=lambda state: log.warning(
                "Attempt %d failed for %s: %s", state.attempt_number, url, state.outcome.exception()
            ),
            reraise=True,
        )
        return retrying(self._get_once, url, params)
