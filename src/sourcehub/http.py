import time
from datetime import timedelta

RETRYABLE = {429, 500, 502, 503, 504}


def _retry_delay(resp, attempt, base_delay, max_delay):
    retry_after = resp.headers.get("Retry-After")
    if retry_after:
        try:
            return float(retry_after)
        except ValueError:
            pass
    return min(max_delay, base_delay * 2**attempt)


def get_with_retry(session, url, params=None, max_retries=5, base_delay=1.0, max_delay=30.0, sleep=time.sleep):
    for attempt in range(max_retries + 1):
        resp = session.get(url, params=params, timeout=30)
        if resp.status_code in RETRYABLE and attempt < max_retries:
            sleep(_retry_delay(resp, attempt, base_delay, max_delay))
            continue
        resp.raise_for_status()
        return resp.json()


def fetch_since(session, base_url, watermark, lookback=timedelta(days=3), page_size=100, **retry):
    # Re-read a lookback window: ad platforms restate recent days after the fact.
    since = watermark - lookback
    params = {"updated_since": since.isoformat(), "limit": page_size}
    url, rows = base_url, []
    while url:
        payload = get_with_retry(session, url, params=params, **retry)
        rows.extend(payload["data"])
        url = payload.get("next")
        params = None
    return rows
