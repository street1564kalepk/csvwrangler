"""Tests for CSVBinner."""

import pytest
from csvwrangler.binner import CSVBinner


class _FakeSource:
    def __init__(self, headers, rows):
        self._headers = headers
        self._rows = rows

    def headers(self):
        return list(self._headers)

    def rows(self):
        return iter([dict(r) for r in self._rows])


BINS = [(0, 18, "child"), (18, 65, "adult"), (65, 200, "senior")]


def _source():
    return _FakeSource(
        ["name", "age"],
        [
            {"name": "Alice", "age": "30"},
            {"name": "Bob", "age": "10"},
            {"name": "Carol", "age": "70"},
            {"name": "Dave", "age": "17"},
            {"name": "Eve", "age": "65"},
            {"name": "Frank", "age": "n/a"},
        ],
    )


def test_headers_include_output_column():
    b = CSVBinner(_source(), "age", BINS)
    assert b.headers() == ["name", "age", "bin"]


def test_custom_output_column_name():
    b = CSVBinner(_source(), "age", BINS, output_column="age_group")
    assert "age_group" in b.headers()


def test_bin_adult():
    b = CSVBinner(_source(), "age", BINS)
    rows = list(b.rows())
    alice = next(r for r in rows if r["name"] == "Alice")
    assert alice["bin"] == "adult"


def test_bin_child():
    b = CSVBinner(_source(), "age", BINS)
    rows = list(b.rows())
    bob = next(r for r in rows if r["name"] == "Bob")
    assert bob["bin"] == "child"


def test_bin_senior():
    b = CSVBinner(_source(), "age", BINS)
    rows = list(b.rows())
    carol = next(r for r in rows if r["name"] == "Carol")
    assert carol["bin"] == "senior"


def test_boundary_low_inclusive_high_exclusive():
    # 65 should NOT be senior (65 < 65 is False), falls into adult (18 <= 65 < 65 False too)
    # With BINS = [(18,65,'adult'),(65,200,'senior')] 65 goes to senior
    b = CSVBinner(_source(), "age", BINS)
    rows = list(b.rows())
    eve = next(r for r in rows if r["name"] == "Eve")
    assert eve["bin"] == "senior"


def test_default_label_for_non_numeric():
    b = CSVBinner(_source(), "age", BINS, default="unknown")
    rows = list(b.rows())
    frank = next(r for r in rows if r["name"] == "Frank")
    assert frank["bin"] == "unknown"


def test_row_count():
    b = CSVBinner(_source(), "age", BINS)
    assert b.row_count() == 6


def test_original_columns_preserved():
    b = CSVBinner(_source(), "age", BINS)
    rows = list(b.rows())
    assert rows[0]["name"] == "Alice"
    assert rows[0]["age"] == "30"


def test_empty_bins_raises():
    with pytest.raises(ValueError, match="bins must not be empty"):
        CSVBinner(_source(), "age", [])


def test_empty_column_raises():
    with pytest.raises(ValueError, match="column must be"):
        CSVBinner(_source(), "", BINS)
