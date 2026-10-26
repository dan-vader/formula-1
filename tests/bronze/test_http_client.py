import pytest
import requests

from formula_1.http_client import PacedClient, RetryableHttpError


class FakeClock:
    """Time that moves only when the client sleeps; every sleep is recorded."""

    def __init__(self):
        self.now = 0.0
        self.sleeps = []

    def time(self):
        return self.now

    def sleep(self, seconds):
        self.sleeps.append(seconds)
        self.now += seconds


class FakeResponse:
    def __init__(self, status_code, body=None, headers=None):
        self.status_code = status_code
        self._body = body
        self.headers = headers or {}

    def json(self):
        return self._body

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(f"HTTP {self.status_code}", response=self)


class FakeSession:
    """Returns prepared responses one by one and counts the requests."""

    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = 0

    def get(self, url, params=None, headers=None, timeout=None):
        self.calls += 1
        return self.responses.pop(0)


def make_client(responses, clock, **overrides):
    session = FakeSession(responses)
    settings = {"min_interval_sec": 0, "max_requests_per_hour": 100, **overrides}
    client = PacedClient("test-agent", session=session, clock=clock.time, sleep=clock.sleep, **settings)
    return client, session


@pytest.mark.parametrize("status_code", [429, 500, 503])
def test_get_json_retries_on_rate_limit_and_server_errors(status_code):
    clock = FakeClock()
    client, session = make_client([FakeResponse(status_code), FakeResponse(200, ["ok"])], clock)

    assert client.get_json("url") == ["ok"]
    assert session.calls == 2


def test_get_json_waits_as_long_as_retry_after_says():
    clock = FakeClock()
    responses = [FakeResponse(429, headers={"Retry-After": "7"}), FakeResponse(200, [])]
    client, _ = make_client(responses, clock)

    client.get_json("url")

    assert clock.sleeps == [7.0]


def test_get_json_fails_fast_on_401_without_retry():
    clock = FakeClock()
    client, session = make_client([FakeResponse(401)], clock)

    with pytest.raises(requests.HTTPError):
        client.get_json("url")
    assert session.calls == 1


def test_get_json_gives_up_after_max_attempts():
    clock = FakeClock()
    client, session = make_client([FakeResponse(500)] * 3, clock, max_attempts=3)

    with pytest.raises(RetryableHttpError):
        client.get_json("url")
    assert session.calls == 3


def test_requests_keep_minimum_interval():
    clock = FakeClock()
    client, _ = make_client([FakeResponse(200, []), FakeResponse(200, [])], clock, min_interval_sec=2.5)

    client.get_json("url")
    client.get_json("url")

    assert clock.sleeps == [2.5]


def test_hourly_budget_makes_client_wait_for_next_slot():
    clock = FakeClock()
    client, _ = make_client([FakeResponse(200, [])] * 3, clock, max_requests_per_hour=2)

    for _ in range(3):
        client.get_json("url")

    assert clock.sleeps == [3600]
