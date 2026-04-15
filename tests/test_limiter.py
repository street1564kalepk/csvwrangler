"""Tests for CSVLimiter."""

import pytest
from csvwrangler.limiter import CSVLimiter


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

class _FakeSource:
    def __init__(self, rows):
        self._headers = list(rows[0].keys()) if rows else []
        self._rows = rows

    @property
    def headers(self):
        return self._headers

    def rows(self):
        yield from self._rows


def _source():
    data = [
        {"id": "1", "name": "Alice"},
        {"id": "2", "name": "Bob"},
        {"id": "3", "name": "Carol"},
        {"id": "4", "name": "Dave"},
        {"id": "5", "name": "Eve"},
    ]
    return _FakeSource(data)


def make_limiter(limit=None, offset=0):
    return CSVLimiter(_source(), limit=limit, offset=offset)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_headers_unchanged():
    limiter = make_limiter()
    assert limiter.headers == ["id", "name"]


def test_no_limit_no_offset_yields_all():
    limiter = make_limiter()
    result = list(limiter.rows())
    assert len(result) == 5
    assert result[0]["name"] == "Alice"
    assert result[-1]["name"] == "Eve"


def test_limit_reduces_rows():
    limiter = make_limiter(limit=3)
    result = list(limiter.rows())
    assert len(result) == 3
    assert [r["name"] for r in result] == ["Alice", "Bob", "Carol"]


def test_offset_skips_rows():
    limiter = make_limiter(offset=2)
    result = list(limiter.rows())
    assert len(result) == 3
    assert result[0]["name"] == "Carol"


def test_limit_and_offset_combined():
    limiter = make_limiter(limit=2, offset=1)
    result = list(limiter.rows())
    assert [r["name"] for r in result] == ["Bob", "Carol"]


def test_row_count_matches_rows():
    limiter = make_limiter(limit=2, offset=1)
    assert limiter.row_count() == 2


def test_offset_beyond_source_yields_nothing():
    limiter = make_limiter(offset=10)
    assert list(limiter.rows()) == []


def test_limit_zero_yields_nothing():
    limiter = make_limiter(limit=0)
    assert list(limiter.rows()) == []


def test_take_returns_new_limiter():
    limiter = make_limiter(offset=1).take(2)
    result = list(limiter.rows())
    assert [r["name"] for r in result] == ["Bob", "Carol"]


def test_skip_returns_new_limiter():
    limiter = make_limiter(limit=4).skip(2)
    result = list(limiter.rows())
    assert [r["name"] for r in result] == ["Carol", "Dave"]


def test_negative_limit_raises():
    with pytest.raises(ValueError, match="limit"):
        CSVLimiter(_source(), limit=-1)


def test_negative_offset_raises():
    with pytest.raises(ValueError, match="offset"):
        CSVLimiter(_source(), offset=-1)
