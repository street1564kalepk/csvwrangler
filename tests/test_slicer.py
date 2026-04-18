"""Tests for CSVSlicer."""
from __future__ import annotations

import pytest

from csvwrangler.slicer import CSVSlicer


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

class _FakeSource:
    def __init__(self, rows: list[dict]) -> None:
        self._rows = rows

    @property
    def headers(self) -> list[str]:
        return list(self._rows[0].keys()) if self._rows else []

    def rows(self):
        yield from self._rows


_DATA = [
    {"id": "1", "name": "Alice"},
    {"id": "2", "name": "Bob"},
    {"id": "3", "name": "Carol"},
    {"id": "4", "name": "Dave"},
    {"id": "5", "name": "Eve"},
]


def _source() -> _FakeSource:
    return _FakeSource(_DATA)


def _make(offset: int = 0, limit=None) -> CSVSlicer:
    return CSVSlicer(_source(), offset=offset, limit=limit)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_headers_unchanged():
    assert _make().headers == ["id", "name"]


def test_no_offset_no_limit_yields_all():
    result = list(_make().rows())
    assert len(result) == 5
    assert result[0]["name"] == "Alice"
    assert result[-1]["name"] == "Eve"


def test_limit_only():
    result = list(_make(limit=3).rows())
    assert [r["name"] for r in result] == ["Alice", "Bob", "Carol"]


def test_offset_only():
    result = list(_make(offset=2).rows())
    assert [r["name"] for r in result] == ["Carol", "Dave", "Eve"]


def test_offset_and_limit():
    result = list(_make(offset=1, limit=2).rows())
    assert [r["name"] for r in result] == ["Bob", "Carol"]


def test_offset_beyond_end_yields_nothing():
    result = list(_make(offset=10).rows())
    assert result == []


def test_limit_zero_yields_nothing():
    result = list(_make(limit=0).rows())
    assert result == []


def test_row_count_matches_rows():
    slicer = _make(offset=1, limit=3)
    assert slicer.row_count() == 3


def test_row_count_partial_at_end():
    # offset=4 → only 1 row left; limit=10 should not exceed available rows
    slicer = _make(offset=4, limit=10)
    assert slicer.row_count() == 1


def test_negative_offset_raises():
    with pytest.raises(ValueError, match="offset"):
        CSVSlicer(_source(), offset=-1)


def test_negative_limit_raises():
    with pytest.raises(ValueError, match="limit"):
        CSVSlicer(_source(), limit=-5)


def test_rows_are_independent_across_iterations():
    """Iterating rows() twice should yield the same results each time."""
    slicer = _make(offset=1, limit=3)
    first = list(slicer.rows())
    second = list(slicer.rows())
    assert [r["name"] for r in first] == [r["name"] for r in second]
