"""Tests for CSVStripper."""
from __future__ import annotations

import pytest

from csvwrangler.stripper import CSVStripper


class _FakeSource:
    def __init__(self, headers, rows):
        self._headers = headers
        self._rows = rows

    @property
    def headers(self):
        return list(self._headers)

    def rows(self):
        yield from self._rows


def _source():
    return _FakeSource(
        headers=["name", "city", "score"],
        rows=[
            {"name": "  Alice  ", "city": " London ", "score": "  10  "},
            {"name": "Bob", "city": "Paris", "score": "20"},
            {"name": "  Carol", "city": "Berlin  ", "score": "30"},
        ],
    )


def test_headers_unchanged():
    s = CSVStripper(_source())
    assert s.headers == ["name", "city", "score"]


def test_strip_all_columns_by_default():
    result = list(CSVStripper(_source()).rows())
    assert result[0]["name"] == "Alice"
    assert result[0]["city"] == "London"
    assert result[0]["score"] == "10"


def test_strip_specific_column_only():
    result = list(CSVStripper(_source(), columns=["name"]).rows())
    assert result[0]["name"] == "Alice"
    # city should remain untouched
    assert result[0]["city"] == " London "


def test_strip_multiple_specific_columns():
    result = list(CSVStripper(_source(), columns=["name", "city"]).rows())
    assert result[0]["name"] == "Alice"
    assert result[0]["city"] == "London"
    assert result[0]["score"] == "  10  "


def test_strip_preserves_non_padded_values():
    result = list(CSVStripper(_source()).rows())
    assert result[1]["name"] == "Bob"
    assert result[1]["city"] == "Paris"


def test_strip_custom_chars():
    src = _FakeSource(
        headers=["tag"],
        rows=[{"tag": "***hello***"}, {"tag": "*world*"}],
    )
    result = list(CSVStripper(src, chars="*").rows())
    assert result[0]["tag"] == "hello"
    assert result[1]["tag"] == "world"


def test_strip_non_string_values_untouched():
    src = _FakeSource(
        headers=["value"],
        rows=[{"value": 42}, {"value": None}],
    )
    result = list(CSVStripper(src).rows())
    assert result[0]["value"] == 42
    assert result[1]["value"] is None


def test_row_count_matches():
    s = CSVStripper(_source())
    assert s.row_count == 3


def test_unknown_column_raises():
    with pytest.raises(ValueError, match="unknown column"):
        CSVStripper(_source(), columns=["nonexistent"])
