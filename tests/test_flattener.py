"""Tests for CSVFlattener."""
from __future__ import annotations

import pytest

from csvwrangler.flattener import CSVFlattener


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

class _FakeSource:
    def __init__(self, headers: list[str], data: list[dict]):
        self._headers = headers
        self._data = data

    @property
    def headers(self) -> list[str]:
        return list(self._headers)

    def rows(self):
        yield from self._data


def _source():
    return _FakeSource(
        headers=["id", "name", "tags"],
        data=[
            {"id": "1", "name": "Alice", "tags": "python|csv|data"},
            {"id": "2", "name": "Bob",   "tags": "java"},
            {"id": "3", "name": "Carol",  "tags": ""},
        ],
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_headers_unchanged():
    f = CSVFlattener(_source(), column="tags")
    assert f.headers == ["id", "name", "tags"]


def test_flatten_splits_multi_value():
    f = CSVFlattener(_source(), column="tags")
    result = list(f.rows())
    # Alice has 3 tags, Bob has 1, Carol has 1 (empty string counts as one part)
    assert len(result) == 5


def test_flatten_values_correct():
    f = CSVFlattener(_source(), column="tags")
    tags = [r["tags"] for r in f.rows()]
    assert tags == ["python", "csv", "data", "java", ""]


def test_other_columns_preserved():
    f = CSVFlattener(_source(), column="tags")
    rows = list(f.rows())
    alice_rows = [r for r in rows if r["name"] == "Alice"]
    assert len(alice_rows) == 3
    assert all(r["id"] == "1" for r in alice_rows)


def test_custom_delimiter():
    src = _FakeSource(
        headers=["id", "vals"],
        data=[{"id": "1", "vals": "a,b,c"}],
    )
    f = CSVFlattener(src, column="vals", delimiter=",")
    result = [r["vals"] for r in f.rows()]
    assert result == ["a", "b", "c"]


def test_strip_whitespace():
    src = _FakeSource(
        headers=["id", "vals"],
        data=[{"id": "1", "vals": " x | y | z "}],
    )
    f = CSVFlattener(src, column="vals", strip=True)
    result = [r["vals"] for r in f.rows()]
    assert result == ["x", "y", "z"]


def test_no_strip():
    src = _FakeSource(
        headers=["id", "vals"],
        data=[{"id": "1", "vals": " x | y "}],
    )
    f = CSVFlattener(src, column="vals", strip=False)
    result = [r["vals"] for r in f.rows()]
    assert result == [" x ", " y "]


def test_row_count():
    f = CSVFlattener(_source(), column="tags")
    assert f.row_count() == 5


def test_invalid_column_raises():
    with pytest.raises(ValueError, match="Column 'missing' not found"):
        CSVFlattener(_source(), column="missing")
