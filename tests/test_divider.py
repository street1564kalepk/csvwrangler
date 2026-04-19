"""Tests for CSVDivider."""
import pytest
from csvwrangler.divider import CSVDivider


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
    return _FakeSource(
        ["name", "score", "total"],
        [
            ["Alice", "100", "200"],
            ["Bob", "75", "150"],
            ["Carol", "50", "300"],
        ],
    )


def test_headers_inplace():
    d = CSVDivider(_source(), ["score"], 10)
    assert d.headers == ["name", "score", "total"]


def test_headers_with_suffix():
    d = CSVDivider(_source(), ["score"], 10, suffix="_div")
    assert "score_div" in d.headers
    assert "score" in d.headers


def test_divide_inplace_values():
    d = CSVDivider(_source(), ["score"], 10)
    rows = list(d.rows())
    assert rows[0]["score"] == "10.0"
    assert rows[1]["score"] == "7.5"


def test_divide_with_precision():
    d = CSVDivider(_source(), ["score"], 3, precision=2)
    rows = list(d.rows())
    assert rows[0]["score"] == "33.33"


def test_divide_with_suffix_preserves_original():
    d = CSVDivider(_source(), ["score"], 10, suffix="_pct")
    rows = list(d.rows())
    assert rows[0]["score"] == "100"  # original untouched
    assert rows[0]["score_pct"] == "10.0"


def test_divide_multiple_columns():
    d = CSVDivider(_source(), ["score", "total"], 10)
    rows = list(d.rows())
    assert rows[2]["score"] == "5.0"
    assert rows[2]["total"] == "30.0"


def test_non_numeric_becomes_empty():
    src = _FakeSource(["name", "score"], [["Alice", "n/a"]])
    d = CSVDivider(src, ["score"], 10)
    rows = list(d.rows())
    assert rows[0]["score"] == ""


def test_zero_divisor_raises():
    with pytest.raises(ValueError, match="non-zero"):
        CSVDivider(_source(), ["score"], 0)


def test_unknown_column_raises():
    with pytest.raises(ValueError, match="unknown columns"):
        CSVDivider(_source(), ["missing"], 10)


def test_row_count():
    d = CSVDivider(_source(), ["score"], 10)
    assert d.row_count == 3
