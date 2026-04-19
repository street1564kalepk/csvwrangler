"""Tests for CSVPadder."""

import pytest
from csvwrangler.padder import CSVPadder


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
        ["name", "dept", "score"],
        [
            {"name": "Alice", "dept": "Eng", "score": "95"},
            {"name": "Bob", "dept": "HR", "score": "80"},
            {"name": "Charlie", "dept": "Eng", "score": "70"},
        ],
    )


def test_headers_unchanged():
    p = CSVPadder(_source(), ["name"], width=10)
    assert p.headers == ["name", "dept", "score"]


def test_pad_left_default():
    p = CSVPadder(_source(), ["name"], width=10)
    rows = list(p.rows())
    assert rows[0]["name"] == "Alice     "
    assert rows[1]["name"] == "Bob       "


def test_pad_right():
    p = CSVPadder(_source(), ["name"], width=10, align="right")
    rows = list(p.rows())
    assert rows[0]["name"] == "     Alice"


def test_pad_center():
    p = CSVPadder(_source(), ["name"], width=9, align="center")
    rows = list(p.rows())
    assert rows[1]["name"] == "   Bob   "


def test_custom_fillchar():
    p = CSVPadder(_source(), ["dept"], width=6, fillchar="-", align="right")
    rows = list(p.rows())
    assert rows[0]["dept"] == "---Eng"


def test_unaffected_columns_unchanged():
    p = CSVPadder(_source(), ["name"], width=10)
    rows = list(p.rows())
    assert rows[0]["score"] == "95"
    assert rows[0]["dept"] == "Eng"


def test_multiple_columns():
    p = CSVPadder(_source(), ["name", "dept"], width=8)
    rows = list(p.rows())
    assert rows[0]["name"] == "Alice   "
    assert rows[0]["dept"] == "Eng     "


def test_invalid_align_raises():
    with pytest.raises(ValueError, match="align must be"):
        CSVPadder(_source(), ["name"], width=10, align="diagonal")


def test_invalid_fillchar_raises():
    with pytest.raises(ValueError, match="fillchar must be a single character"):
        CSVPadder(_source(), ["name"], width=10, fillchar="--")


def test_invalid_width_raises():
    with pytest.raises(ValueError, match="width must be"):
        CSVPadder(_source(), ["name"], width=0)


def test_unknown_column_raises():
    with pytest.raises(KeyError):
        CSVPadder(_source(), ["nonexistent"], width=10)


def test_row_count():
    p = CSVPadder(_source(), ["name"], width=10)
    assert p.row_count == 3
