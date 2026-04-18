"""Tests for CSVGrouper."""
import pytest
from csvwrangler.grouper import CSVGrouper


class _FakeSource:
    def __init__(self, headers, data):
        self._headers = headers
        self._data = data

    @property
    def headers(self):
        return self._headers

    def rows(self):
        for row in self._data:
            yield dict(zip(self._headers, row))


def _source():
    return _FakeSource(
        ["dept", "salary"],
        [
            ("eng", "90000"),
            ("eng", "80000"),
            ("hr", "60000"),
            ("hr", "65000"),
            ("hr", "70000"),
            ("eng", "95000"),
        ],
    )


def test_headers_group_count():
    g = CSVGrouper(_source(), "dept", "salary", "count")
    assert g.headers == ["dept", "count_salary"]


def test_headers_custom_out_col():
    g = CSVGrouper(_source(), "dept", "salary", "sum", out_col="total")
    assert g.headers == ["dept", "total"]


def test_count_values():
    g = CSVGrouper(_source(), "dept", "salary", "count")
    result = {r["dept"]: r["count_salary"] for r in g.rows()}
    assert result["eng"] == 3
    assert result["hr"] == 3


def test_sum_values():
    g = CSVGrouper(_source(), "dept", "salary", "sum")
    result = {r["dept"]: r["sum_salary"] for r in g.rows()}
    assert result["eng"] == pytest.approx(265000.0)
    assert result["hr"] == pytest.approx(195000.0)


def test_mean_values():
    g = CSVGrouper(_source(), "dept", "salary", "mean")
    result = {r["dept"]: r["mean_salary"] for r in g.rows()}
    assert result["eng"] == pytest.approx(265000 / 3)


def test_min_values():
    g = CSVGrouper(_source(), "dept", "salary", "min")
    result = {r["dept"]: r["min_salary"] for r in g.rows()}
    assert result["eng"] == pytest.approx(80000.0)


def test_max_values():
    g = CSVGrouper(_source(), "dept", "salary", "max")
    result = {r["dept"]: r["max_salary"] for r in g.rows()}
    assert result["hr"] == pytest.approx(70000.0)


def test_row_count():
    g = CSVGrouper(_source(), "dept", "salary", "count")
    assert g.row_count == 2


def test_invalid_agg_func():
    with pytest.raises(ValueError, match="agg_func"):
        CSVGrouper(_source(), "dept", "salary", "median")


def test_invalid_group_col():
    with pytest.raises(ValueError, match="group_col"):
        CSVGrouper(_source(), "nope", "salary")


def test_invalid_agg_col():
    with pytest.raises(ValueError, match="agg_col"):
        CSVGrouper(_source(), "dept", "nope")
