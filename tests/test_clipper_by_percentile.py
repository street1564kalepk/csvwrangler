"""Tests for CSVPercentileClipper."""

import pytest
from csvwrangler.clipper_by_percentile import CSVPercentileClipper


class _FakeSource:
    def __init__(self, headers, rows):
        self._headers = headers
        self._rows = rows

    @property
    def headers(self):
        return self._headers

    @property
    def rows(self):
        return iter(self._rows)


def _source():
    """10 rows with score 10, 20, …, 100."""
    hdrs = ["name", "score"]
    data = [{"name": f"r{i}", "score": str(i * 10)} for i in range(1, 11)]
    return _FakeSource(hdrs, data)


def test_headers_unchanged():
    clip = CSVPercentileClipper(_source(), "score")
    assert clip.headers == ["name", "score"]


def test_default_keeps_all_rows():
    clip = CSVPercentileClipper(_source(), "score")
    assert clip.row_count == 10


def test_top_50_pct():
    """p50–p100 should keep rows with score >= median."""
    clip = CSVPercentileClipper(_source(), "score", low_pct=50.0, high_pct=100.0)
    scores = [int(r["score"]) for r in clip.rows]
    assert all(s >= 50 for s in scores)


def test_bottom_50_pct():
    clip = CSVPercentileClipper(_source(), "score", low_pct=0.0, high_pct=50.0)
    scores = [int(r["score"]) for r in clip.rows]
    assert all(s <= 55 for s in scores)  # interpolation may land at 55


def test_narrow_band():
    clip = CSVPercentileClipper(_source(), "score", low_pct=40.0, high_pct=60.0)
    results = list(clip.rows)
    assert len(results) >= 1


def test_invalid_percentile_raises():
    with pytest.raises(ValueError, match="between 0 and 100"):
        CSVPercentileClipper(_source(), "score", low_pct=-5)


def test_inverted_bounds_raises():
    with pytest.raises(ValueError, match="low_pct must be"):
        CSVPercentileClipper(_source(), "score", low_pct=80, high_pct=20)


def test_non_numeric_rows_skipped():
    hdrs = ["name", "score"]
    data = [
        {"name": "alice", "score": "50"},
        {"name": "bob", "score": "N/A"},
        {"name": "carol", "score": "80"},
    ]
    src = _FakeSource(hdrs, data)
    clip = CSVPercentileClipper(src, "score", low_pct=0, high_pct=100)
    names = [r["name"] for r in clip.rows]
    assert "bob" not in names
    assert "alice" in names
    assert "carol" in names


def test_row_count_property():
    clip = CSVPercentileClipper(_source(), "score", low_pct=90.0, high_pct=100.0)
    assert clip.row_count == len(list(clip.rows))
