"""Tests for CSVRoller."""
import pytest
from csvwrangler.roller import CSVRoller


class _FakeSource:
    def __init__(self, hdrs, data):
        self._headers = hdrs
        self._data = data

    @property
    def headers(self):
        return list(self._headers)

    def rows(self):
        for row in self._data:
            yield dict(row)


def _source():
    return _FakeSource(
        ["name", "value"],
        [
            {"name": "a", "value": "10"},
            {"name": "b", "value": "20"},
            {"name": "c", "value": "30"},
            {"name": "d", "value": "40"},
            {"name": "e", "value": "50"},
        ],
    )


def test_headers_include_new_column():
    r = CSVRoller(_source(), "value", window=3, agg="mean")
    assert r.headers == ["name", "value", "value_rolling_mean"]


def test_custom_out_col():
    r = CSVRoller(_source(), "value", window=2, agg="sum", out_col="val_sum")
    assert "val_sum" in r.headers


def test_rolling_mean_window2():
    r = CSVRoller(_source(), "value", window=2, agg="mean")
    results = [row["value_rolling_mean"] for row in r.rows()]
    assert results[0] == pytest.approx(10.0)
    assert results[1] == pytest.approx(15.0)
    assert results[2] == pytest.approx(25.0)
    assert results[3] == pytest.approx(35.0)
    assert results[4] == pytest.approx(45.0)


def test_rolling_sum_window3():
    r = CSVRoller(_source(), "value", window=3, agg="sum")
    results = [row["value_rolling_sum"] for row in r.rows()]
    assert results[0] == pytest.approx(10.0)
    assert results[1] == pytest.approx(30.0)
    assert results[2] == pytest.approx(60.0)
    assert results[3] == pytest.approx(90.0)
    assert results[4] == pytest.approx(120.0)


def test_rolling_min():
    r = CSVRoller(_source(), "value", window=3, agg="min")
    results = [row["value_rolling_min"] for row in r.rows()]
    assert results[4] == pytest.approx(30.0)


def test_rolling_max():
    r = CSVRoller(_source(), "value", window=3, agg="max")
    results = [row["value_rolling_max"] for row in r.rows()]
    assert results[4] == pytest.approx(50.0)


def test_invalid_column_raises():
    with pytest.raises(ValueError, match="not found"):
        CSVRoller(_source(), "nonexistent", window=2)


def test_invalid_window_raises():
    with pytest.raises(ValueError, match="window"):
        CSVRoller(_source(), "value", window=0)


def test_invalid_agg_raises():
    with pytest.raises(ValueError, match="agg"):
        CSVRoller(_source(), "value", window=2, agg="median")


def test_original_columns_preserved():
    r = CSVRoller(_source(), "value", window=2, agg="mean")
    rows = list(r.rows())
    assert rows[0]["name"] == "a"
    assert rows[0]["value"] == "10"
