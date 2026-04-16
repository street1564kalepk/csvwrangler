"""Tests for CSVFiller."""

import pytest
from csvwrangler.filler import CSVFiller


class _FakeSource:
    def __init__(self, headers, data):
        self._headers = headers
        self._data = data

    @property
    def headers(self):
        return list(self._headers)

    def rows(self):
        yield from (dict(r) for r in self._data)


def _source():
    return _FakeSource(
        headers=["name", "age", "city"],
        data=[
            {"name": "Alice", "age": "30", "city": "London"},
            {"name": "", "age": "", "city": "Paris"},
            {"name": "Bob", "age": None, "city": ""},
            {"name": "", "age": "25", "city": None},
        ],
    )


def test_headers_unchanged():
    filler = CSVFiller(_source(), fill_values={"name": "Unknown", "city": "N/A"})
    assert filler.headers == ["name", "age", "city"]


def test_fills_empty_string():
    filler = CSVFiller(_source(), fill_values={"name": "Unknown"})
    names = [r["name"] for r in filler.rows()]
    assert names == ["Alice", "Unknown", "Bob", "Unknown"]


def test_fills_none():
    filler = CSVFiller(_source(), fill_values={"age": "0"})
    ages = [r["age"] for r in filler.rows()]
    assert ages == ["30", "0", "0", "25"]


def test_fills_multiple_columns():
    filler = CSVFiller(
        _source(), fill_values={"name": "Unknown", "city": "N/A", "age": "0"}
    )
    rows = list(filler.rows())
    assert rows[1] == {"name": "Unknown", "age": "0", "city": "Paris"}
    assert rows[2] == {"name": "Bob", "age": "0", "city": "N/A"}


def test_callable_fill_value():
    counter = {"n": 0}

    def _next():
        counter["n"] += 1
        return f"gen_{counter['n']}"

    filler = CSVFiller(_source(), fill_values={"name": _next})
    names = [r["name"] for r in filler.rows()]
    assert names == ["Alice", "gen_1", "Bob", "gen_2"]


def test_column_subset():
    # Only fill 'city', even though 'name' fill value is also provided
    filler = CSVFiller(
        _source(),
        fill_values={"name": "Unknown", "city": "N/A"},
        columns=["city"],
    )
    rows = list(filler.rows())
    # name should remain empty for rows where it was empty
    assert rows[1]["name"] == ""
    assert rows[1]["city"] == "N/A"


def test_row_count():
    filler = CSVFiller(_source(), fill_values={"name": "X"})
    assert filler.row_count() == 4


def test_unknown_column_raises():
    with pytest.raises(ValueError, match="not found in source headers"):
        CSVFiller(_source(), fill_values={"nonexistent": "X"})


def test_missing_fill_spec_raises():
    with pytest.raises(ValueError, match="no fill value provided"):
        CSVFiller(
            _source(),
            fill_values={"name": "X"},
            columns=["name", "city"],
        )
