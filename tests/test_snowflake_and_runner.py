import pytest
from sourcehub.runner import IngestionFailed, raise_if_failed, run_sources
from sourcehub.snowflake_extract import extract_incremental, incremental_query


class FakeCursor:
    def __init__(self, rows):
        self.rows = list(rows)
        self.executed = None
        self.closed = False

    def execute(self, sql, params):
        self.executed = (sql, params)

    def fetchmany(self, size):
        batch, self.rows = self.rows[:size], self.rows[size:]
        return batch

    def close(self):
        self.closed = True


class FakeConn:
    def __init__(self, cursor):
        self._cursor = cursor

    def cursor(self):
        return self._cursor


def test_query_binds_watermark_instead_of_formatting_it():
    sql = incremental_query("SALES.PUBLIC.ORDERS", "UPDATED_AT", ["ORDER_ID", "AMOUNT"])
    assert sql == "SELECT ORDER_ID, AMOUNT FROM SALES.PUBLIC.ORDERS WHERE UPDATED_AT > %(since)s ORDER BY UPDATED_AT"


def test_rejects_injection_in_identifiers():
    with pytest.raises(ValueError):
        incremental_query("ORDERS; DROP TABLE X", "UPDATED_AT", ["ORDER_ID"])


def test_streams_in_batches_and_closes_cursor():
    cur = FakeCursor([(1, 10.0), (2, 20.0), (3, 30.0)])
    batches = list(extract_incremental(FakeConn(cur), "ORDERS", "UPDATED_AT", ["ORDER_ID", "AMOUNT"], "2026-10-01", batch_size=2))
    assert batches == [[{"ORDER_ID": 1, "AMOUNT": 10.0}, {"ORDER_ID": 2, "AMOUNT": 20.0}], [{"ORDER_ID": 3, "AMOUNT": 30.0}]]
    assert cur.executed[1] == {"since": "2026-10-01"}
    assert cur.closed


def test_one_failing_source_does_not_stop_the_others_but_is_reported():
    def broken():
        raise ConnectionError("token expired")

    results, failures = run_sources({"store_a": lambda: [1], "store_b": broken, "store_c": lambda: [3]})
    assert results == {"store_a": [1], "store_c": [3]}
    assert list(failures) == ["store_b"]
    with pytest.raises(IngestionFailed, match="store_b"):
        raise_if_failed(failures)
