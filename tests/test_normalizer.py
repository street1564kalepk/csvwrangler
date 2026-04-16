"""Tests for CSVNormalizer."""

import pytest
from csvwrangler.normalizer import CSVNormalizer


class _FakeSource:
    def __init__(self, headers, data):
        self._headers = headers
        self._data = data

    @property
    def headers(self):
        return list(self._headers)

    def rows(self):
        for row in self._data:
            yield dict(row)


def _source():
    return _FakeSource(
        ["name", "city", "code"],
        [
            {"name": "  Alice ", "city": "NEW YORK", "code": "A1 "},
            {"name": "BOB", "city": "  london  ", "code": " B2"},
            {"name": "  carol  ann  ", "city": "Paris", "code": "C3"},
        ],
    )


def test_headers_unchanged():
    n = CSVNormalizer(_source(), ["name"], ["strip"])
    assert n.headers == ["name", "city", "code"]


def test_strip():
    rows = list(CSVNormalizer(_source(), ["name", "code"], ["strip"]).rows())
    assert rows[0]["name"] == "Alice"
    assert rows[1]["code"] == "B2"
    assert rows[0]["city"] == "NEW YORK"  # untouched


def test_lower():
    rows = list(CSVNormalizer(_source(), ["city"], ["lower"]).rows())
    assert rows[0]["city"] == "new york"
    assert rows[1]["city"] == "  london  "  # strip not applied


def test_upper():
    rows = list(CSVNormalizer(_source(), ["name"], ["strip", "upper"]).rows())
    assert rows[0]["name"] == "ALICE"
    assert rows[2]["name"] == "CAROL  ANN"


def test_title():
    rows = list(CSVNormalizer(_source(), ["city"], ["strip", "title"]).rows())
    assert rows[1]["city"] == "London"


def test_collapse():
    rows = list(CSVNormalizer(_source(), ["name"], ["collapse"]).rows())
    assert rows[2]["name"] == "carol ann"


def test_chained_strip_lower():
    rows = list(CSVNormalizer(_source(), ["city"], ["strip", "lower"]).rows())
    assert rows[1]["city"] == "london"


def test_unknown_operation_raises():
    with pytest.raises(ValueError, match="Unknown normalisation"):
        CSVNormalizer(_source(), ["name"], ["bogus"])


def test_unknown_column_raises():
    with pytest.raises(ValueError, match="Columns not found"):
        CSVNormalizer(_source(), ["missing"], ["strip"])


def test_row_count():
    n = CSVNormalizer(_source(), ["name"], ["strip"])
    assert n.row_count == 3
