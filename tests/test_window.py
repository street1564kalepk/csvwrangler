"""Tests for CSVWindow."""
import pytest
from csvwrangler.window import CSVWindow


class _FakeSource:
    def __init__(self, hdrs, data):
        self._headers = hdrs
        self._data = data

    @property
    def headers(self):
        return list(self._headers)

    def rows(self):
        for r in self._data:
            yield dict(r)


def _source():
    return _FakeSource(
        ["name", "value"],
        [
            {"name": "a", "value": "10"},
            {"name": "b", "value": "20"},
            {"name": "c", "value": "30"},
            {"name": "d", "value": "40"},
        ],
    )


def test_headers_include_new_column():
    w = CSVWindow(_source(), "value", 2, "sum")
    assert "value_sum_2" in w.headers
    assert "name" in w.headers


def test_custom_output_name():
    w = CSVWindow(_source(), "value", 2, "sum", output="rolling")
    assert "rolling" in w.headers


def test_rolling_sum():
    w = CSVWindow(_source(), "value", 2, "sum")
    results = [r["value_sum_2"] for r in w.rows()]
    assert results == ["10.0", "30.0", "50.0", "70.0"]


def test_rolling_mean():
    w = CSVWindow(_source(), "value", 2, "mean")
    results = [r["value_mean_2"] for r in w.rows()]
    assert results[0] == "10.0"
    assert results[1] == "15.0"
    assert results[2] == "25.0"


def test_rolling_min():
    w = CSVWindow(_source(), "value", 3, "min")
    results = [r["value_min_3"] for r in w.rows()]
    assert results[2] == "10.0"
    assert results[3] == "20.0"


def test_rolling_max():
    w = CSVWindow(_source(), "value", 3, "max")
    results = [r["value_max_3"] for r in w.rows()]
    assert results[2] == "30.0"


def test_rolling_count():
    w = CSVWindow(_source(), "value", 2, "count")
    results = [r["value_count_2"] for r in w.rows()]
    assert results == ["1", "2", "2", "2"]


def test_invalid_func_raises():
    with pytest.raises(ValueError, match="func must be"):
        CSVWindow(_source(), "value", 2, "median")


def test_invalid_size_raises():
    with pytest.raises(ValueError, match="size must be"):
        CSVWindow(_source(), "value", 0, "sum")


def test_row_count():
    w = CSVWindow(_source(), "value", 2, "sum")
    assert w.row_count == 4
