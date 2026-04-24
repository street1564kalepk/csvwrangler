"""Tests for CSVCompactor."""
from __future__ import annotations

import pytest
from csvwrangler.compactor import CSVCompactor


class _FakeSource:
    def __init__(self, headers: list[str], rows: list[dict]) -> None:
        self._headers = headers
        self._rows = rows

    @property
    def headers(self) -> list[str]:
        return list(self._headers)

    @property
    def rows(self):
        yield from self._rows


def _source(headers, rows):
    return _FakeSource(headers, rows)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

MIXED_HEADERS = ["name", "empty_col", "score"]
MIXED_ROWS = [
    {"name": "Alice", "empty_col": "", "score": "90"},
    {"name": "Bob",   "empty_col": "", "score": "85"},
    {"name": "Carol", "empty_col": " ", "score": "78"},
]

PARTIAL_HEADERS = ["name", "nickname", "score"]
PARTIAL_ROWS = [
    {"name": "Alice", "nickname": "",     "score": "90"},
    {"name": "Bob",   "nickname": "Bobby", "score": "85"},
]


# ---------------------------------------------------------------------------
# keep_if_any=True (default)
# ---------------------------------------------------------------------------

def test_headers_unchanged_when_all_have_data():
    src = _source(["a", "b"], [{"a": "1", "b": "2"}, {"a": "3", "b": "4"}])
    c = CSVCompactor(src)
    assert c.headers == ["a", "b"]


def test_fully_empty_column_is_dropped():
    src = _source(MIXED_HEADERS, MIXED_ROWS)
    c = CSVCompactor(src)
    assert "empty_col" not in c.headers
    assert "name" in c.headers
    assert "score" in c.headers


def test_partially_empty_column_kept_with_keep_if_any():
    src = _source(PARTIAL_HEADERS, PARTIAL_ROWS)
    c = CSVCompactor(src)  # keep_if_any=True
    assert "nickname" in c.headers


def test_row_count_preserved_after_compaction():
    src = _source(MIXED_HEADERS, MIXED_ROWS)
    c = CSVCompactor(src)
    assert c.row_count == len(MIXED_ROWS)


def test_rows_only_contain_kept_columns():
    src = _source(MIXED_HEADERS, MIXED_ROWS)
    c = CSVCompactor(src)
    for row in c.rows:
        assert set(row.keys()) == set(c.headers)


def test_data_values_preserved():
    src = _source(MIXED_HEADERS, MIXED_ROWS)
    c = CSVCompactor(src)
    result = list(c.rows)
    assert result[0]["name"] == "Alice"
    assert result[0]["score"] == "90"


# ---------------------------------------------------------------------------
# keep_if_any=False
# ---------------------------------------------------------------------------

def test_partially_empty_column_dropped_when_all_required():
    src = _source(PARTIAL_HEADERS, PARTIAL_ROWS)
    c = CSVCompactor(src, keep_if_any=False)
    assert "nickname" not in c.headers


def test_fully_populated_column_kept_when_all_required():
    src = _source(PARTIAL_HEADERS, PARTIAL_ROWS)
    c = CSVCompactor(src, keep_if_any=False)
    assert "name" in c.headers
    assert "score" in c.headers


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------

def test_no_rows_returns_all_headers_empty_data():
    src = _source(["x", "y"], [])
    c = CSVCompactor(src)
    assert c.headers == ["x", "y"]
    assert c.row_count == 0


def test_whitespace_only_treated_as_empty():
    src = _source(["col"], [{"col": "   "}, {"col": "\t"}])
    c = CSVCompactor(src)
    assert "col" not in c.headers
