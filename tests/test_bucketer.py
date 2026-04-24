"""Tests for CSVBucketer."""
import pytest
from csvwrangler.bucketer import CSVBucketer


class _FakeSource:
    def __init__(self, headers, rows):
        self._headers = headers
        self._rows = rows

    @property
    def headers(self):
        return list(self._headers)

    def rows(self):
        return iter([dict(r) for r in self._rows])


def _source():
    return _FakeSource(
        headers=["name", "score"],
        rows=[
            {"name": "Alice", "score": "15"},
            {"name": "Bob", "score": "45"},
            {"name": "Carol", "score": "75"},
            {"name": "Dave", "score": "95"},
            {"name": "Eve", "score": "bad"},
        ],
    )


_BUCKETS = [("low", 25.0), ("mid", 60.0), ("high", 85.0)]


def test_headers_appended():
    b = CSVBucketer(_source(), "score", _BUCKETS)
    assert b.headers == ["name", "score", "bucket"]


def test_headers_custom_target_column():
    b = CSVBucketer(_source(), "score", _BUCKETS, target_column="tier")
    assert "tier" in b.headers
    assert "bucket" not in b.headers


def test_low_bucket():
    b = CSVBucketer(_source(), "score", _BUCKETS)
    rows = list(b.rows())
    assert rows[0]["bucket"] == "low"


def test_mid_bucket():
    b = CSVBucketer(_source(), "score", _BUCKETS)
    rows = list(b.rows())
    assert rows[1]["bucket"] == "mid"


def test_high_bucket():
    b = CSVBucketer(_source(), "score", _BUCKETS)
    rows = list(b.rows())
    assert rows[2]["bucket"] == "high"


def test_default_bucket_exceeds_all_bounds():
    b = CSVBucketer(_source(), "score", _BUCKETS)
    rows = list(b.rows())
    assert rows[3]["bucket"] == "other"


def test_default_bucket_non_numeric():
    b = CSVBucketer(_source(), "score", _BUCKETS)
    rows = list(b.rows())
    assert rows[4]["bucket"] == "other"


def test_custom_default_label():
    b = CSVBucketer(_source(), "score", _BUCKETS, default="unknown")
    rows = list(b.rows())
    assert rows[3]["bucket"] == "unknown"


def test_row_count():
    b = CSVBucketer(_source(), "score", _BUCKETS)
    assert b.row_count == 5


def test_original_data_preserved():
    b = CSVBucketer(_source(), "score", _BUCKETS)
    rows = list(b.rows())
    assert rows[0]["name"] == "Alice"
    assert rows[0]["score"] == "15"


def test_empty_buckets_raises():
    with pytest.raises(ValueError):
        CSVBucketer(_source(), "score", [])


def test_missing_column_raises():
    with pytest.raises(KeyError):
        CSVBucketer(_source(), "nonexistent", _BUCKETS)


def test_buckets_sorted_by_upper_bound():
    # Provide buckets in reverse order – should still classify correctly.
    shuffled = [("high", 85.0), ("low", 25.0), ("mid", 60.0)]
    b = CSVBucketer(_source(), "score", shuffled)
    rows = list(b.rows())
    assert rows[0]["bucket"] == "low"
    assert rows[1]["bucket"] == "mid"
    assert rows[2]["bucket"] == "high"
