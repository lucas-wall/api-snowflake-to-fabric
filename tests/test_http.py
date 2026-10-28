from datetime import datetime, timedelta, timezone

import pytest
import requests
from fakes import FakeResponse, FakeSession
from sourcehub.http import fetch_since, get_with_retry


def test_honours_retry_after_on_rate_limit():
    sleeps = []
    session = FakeSession([FakeResponse(429, headers={"Retry-After": "2"}), FakeResponse(200, {"ok": True})])
    assert get_with_retry(session, "u", sleep=sleeps.append) == {"ok": True}
    assert sleeps == [2.0]


def test_exponential_backoff_on_server_errors():
    sleeps = []
    session = FakeSession([FakeResponse(503), FakeResponse(503), FakeResponse(200, {"ok": True})])
    get_with_retry(session, "u", base_delay=1.0, sleep=sleeps.append)
    assert sleeps == [1.0, 2.0]


def test_gives_up_after_max_retries():
    session = FakeSession([FakeResponse(429)] * 4)
    with pytest.raises(requests.HTTPError):
        get_with_retry(session, "u", max_retries=3, sleep=lambda _: None)


def test_client_errors_fail_fast_without_retry():
    session = FakeSession([FakeResponse(401)])
    with pytest.raises(requests.HTTPError):
        get_with_retry(session, "u", sleep=lambda _: None)
    assert len(session.calls) == 1


def test_follows_next_links_and_applies_lookback():
    session = FakeSession([
        FakeResponse(200, {"data": [{"id": 1}], "next": "https://api/x?cursor=abc"}),
        FakeResponse(200, {"data": [{"id": 2}], "next": None}),
    ])
    watermark = datetime(2026, 10, 10, tzinfo=timezone.utc)
    rows = fetch_since(session, "https://api/x", watermark, lookback=timedelta(days=3))
    assert rows == [{"id": 1}, {"id": 2}]
    first_params = session.calls[0][1]
    assert first_params["updated_since"] == "2026-10-07T00:00:00+00:00"
    assert session.calls[1] == ("https://api/x?cursor=abc", None)
