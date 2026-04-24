"""Tests for CSVValueClipper."""

import pytest
from csvwrangler.clipper_by_value import CSVValueClipper


class _FakeSource:
    def __init__(self, headers, rows):
        self._headers = headers
        self._rows = rows

    @property
    def headers(self):
        return list(self._headers)

    def rows(self):
        yield from (dict(r) for r in self._rows)


def _source():
    return _FakeSource(
        ["name", "score", "age"],
        [
            {"name": "Alice", "score": "95", "age": "30"},
            {"name": "Bob",   "score": "42", "age": "17"},
            {"name": "Carol", "score": "110", "age": "25"},
            {"name": "Dave",  "score": "-5",  "age": "22"},
        ],
    )


def test_headers_unchanged():
    c = CSVValueClipper(_source(), ["score"], min_val=0, max_val=100)
    assert c.headers == ["name", "score", "age"]


def test_clip_above_max():
    c = CSVValueClipper(_source(), ["score"], min_val=0, max_val=100)
    result = list(c.rows())
    carol = next(r for r in result if r["name"] == "Carol")
    assert carol["score"] == "100"


def test_clip_below_min():
    c = CSVValueClipper(_source(), ["score"], min_val=0, max_val=100)
    result = list(c.rows())
    dave = next(r for r in result if r["name"] == "Dave")
    assert dave["score"] == "0"


def test_value_within_range_unchanged():
    c = CSVValueClipper(_source(), ["score"], min_val=0, max_val=100)
    result = list(c.rows())
    alice = next(r for r in result if r["name"] == "Alice")
    assert alice["score"] == "95"


def test_non_numeric_left_unchanged():
    src = _FakeSource(
        ["name", "score"],
        [{"name": "Alice", "score": "n/a"}],
    )
    c = CSVValueClipper(src, ["score"], min_val=0, max_val=100)
    result = list(c.rows())
    assert result[0]["score"] == "n/a"


def test_only_specified_columns_clipped():
    c = CSVValueClipper(_source(), ["score"], min_val=0, max_val=100)
    result = list(c.rows())
    bob = next(r for r in result if r["name"] == "Bob")
    # age=17 is below 0 but NOT in clipped columns
    assert bob["age"] == "17"


def test_no_lower_bound():
    c = CSVValueClipper(_source(), ["score"], max_val=100)
    result = list(c.rows())
    dave = next(r for r in result if r["name"] == "Dave")
    assert dave["score"] == "-5"


def test_no_upper_bound():
    c = CSVValueClipper(_source(), ["score"], min_val=0)
    result = list(c.rows())
    carol = next(r for r in result if r["name"] == "Carol")
    assert carol["score"] == "110"


def test_empty_columns_raises():
    with pytest.raises(ValueError, match="columns must not be empty"):
        CSVValueClipper(_source(), [])


def test_min_greater_than_max_raises():
    with pytest.raises(ValueError, match="min_val"):
        CSVValueClipper(_source(), ["score"], min_val=100, max_val=0)


def test_row_count():
    c = CSVValueClipper(_source(), ["score"], min_val=0, max_val=100)
    assert c.row_count == 4
