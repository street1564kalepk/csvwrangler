"""Tests for CSVScaler."""
import pytest
from csvwrangler.scaler import CSVScaler


class _FakeSource:
    def __init__(self, headers, data):
        self._headers = headers
        self._data = data

    @property
    def headers(self):
        return list(self._headers)

    def rows(self):
        return (dict(r) for r in self._data)


def _source():
    return _FakeSource(
        ["name", "score", "age"],
        [
            {"name": "Alice", "score": "10", "age": "20"},
            {"name": "Bob",   "score": "20", "age": "30"},
            {"name": "Carol", "score": "30", "age": "40"},
        ],
    )


def test_headers_unchanged():
    s = CSVScaler(_source(), ["score"])
    assert s.headers == ["name", "score", "age"]


def test_row_count():
    s = CSVScaler(_source(), ["score"])
    assert s.row_count == 3


def test_minmax_min_row():
    s = CSVScaler(_source(), ["score"], method="minmax")
    rows = list(s.rows())
    assert float(rows[0]["score"]) == pytest.approx(0.0)


def test_minmax_max_row():
    s = CSVScaler(_source(), ["score"], method="minmax")
    rows = list(s.rows())
    assert float(rows[2]["score"]) == pytest.approx(1.0)


def test_minmax_mid_row():
    s = CSVScaler(_source(), ["score"], method="minmax")
    rows = list(s.rows())
    assert float(rows[1]["score"]) == pytest.approx(0.5)


def test_zscore_mean_is_zero():
    s = CSVScaler(_source(), ["score"], method="zscore")
    rows = list(s.rows())
    vals = [float(r["score"]) for r in rows]
    assert sum(vals) == pytest.approx(0.0, abs=1e-5)


def test_non_scaled_column_unchanged():
    s = CSVScaler(_source(), ["score"])
    rows = list(s.rows())
    assert rows[0]["name"] == "Alice"
    assert rows[0]["age"] == "20"


def test_invalid_method_raises():
    with pytest.raises(ValueError, match="method must be one of"):
        CSVScaler(_source(), ["score"], method="bad")


def test_non_numeric_value_passed_through():
    src = _FakeSource(
        ["name", "score"],
        [{"name": "Alice", "score": "n/a"}, {"name": "Bob", "score": "n/a"}],
    )
    s = CSVScaler(src, ["score"], method="minmax")
    rows = list(s.rows())
    assert rows[0]["score"] == "n/a"


def test_multiple_columns_scaled():
    s = CSVScaler(_source(), ["score", "age"], method="minmax")
    rows = list(s.rows())
    assert float(rows[0]["score"]) == pytest.approx(0.0)
    assert float(rows[0]["age"]) == pytest.approx(0.0)
    assert float(rows[2]["score"]) == pytest.approx(1.0)
    assert float(rows[2]["age"]) == pytest.approx(1.0)
