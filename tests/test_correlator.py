"""Tests for CSVCorrelator."""
import math
import pytest
from csvwrangler.correlator import CSVCorrelator


class _FakeSource:
    def __init__(self, headers, data):
        self._headers = headers
        self._data = data

    @property
    def headers(self):
        return list(self._headers)

    def rows(self):
        for row in self._data:
            yield dict(zip(self._headers, row))


def _source():
    # x and y are perfectly correlated; z is anti-correlated with x
    return _FakeSource(
        ["x", "y", "z"],
        [
            ["1", "2", "9"],
            ["2", "4", "8"],
            ["3", "6", "7"],
            ["4", "8", "6"],
            ["5", "10", "5"],
        ],
    )


def test_headers_unchanged():
    c = CSVCorrelator(_source())
    assert c.headers == ["x", "y", "z"]


def test_rows_passthrough():
    c = CSVCorrelator(_source())
    rows = list(c.rows())
    assert len(rows) == 5
    assert rows[0]["x"] == "1"


def test_perfect_positive_correlation():
    c = CSVCorrelator(_source(), columns=["x", "y"])
    m = c.matrix
    assert math.isclose(m["x"]["y"], 1.0, abs_tol=1e-9)
    assert math.isclose(m["y"]["x"], 1.0, abs_tol=1e-9)


def test_self_correlation_is_one():
    c = CSVCorrelator(_source())
    m = c.matrix
    for col in ["x", "y", "z"]:
        assert math.isclose(m[col][col], 1.0, abs_tol=1e-9)


def test_anti_correlation():
    c = CSVCorrelator(_source(), columns=["x", "z"])
    m = c.matrix
    assert math.isclose(m["x"]["z"], -1.0, abs_tol=1e-9)


def test_matrix_keys_match_columns():
    cols = ["x", "y"]
    c = CSVCorrelator(_source(), columns=cols)
    m = c.matrix
    assert set(m.keys()) == set(cols)
    for v in m.values():
        assert set(v.keys()) == set(cols)


def test_nan_for_constant_column():
    src = _FakeSource(["a", "b"], [["5", "1"], ["5", "2"], ["5", "3"]])
    c = CSVCorrelator(src)
    m = c.matrix
    assert math.isnan(m["a"]["b"])


def test_non_numeric_values_skipped():
    src = _FakeSource(["x", "y"], [["1", "2"], ["bad", "4"], ["3", "6"]])
    c = CSVCorrelator(src, columns=["x", "y"])
    m = c.matrix
    # x has [1,3], y has [2,4,6] – pearson uses min length → [2,4]
    assert math.isclose(m["x"]["y"], 1.0, abs_tol=1e-9)
