"""Tests for CSVDater."""
from __future__ import annotations

import pytest

from csvwrangler.dater import CSVDater


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

class _FakeSource:
    def __init__(self, headers, rows):
        self._headers = headers
        self._rows = rows
        self.row_count = len(rows)

    @property
    def headers(self):
        return list(self._headers)

    def rows(self):
        yield from (dict(r) for r in self._rows)


def _source():
    return _FakeSource(
        headers=["name", "dob", "joined"],
        rows=[
            {"name": "Alice", "dob": "1990-04-15", "joined": "2020-01-10"},
            {"name": "Bob",   "dob": "1985-11-03", "joined": "2019-07-22"},
            {"name": "Carol", "dob": "2000-06-30", "joined": "2021-03-05"},
        ],
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_headers_unchanged():
    d = CSVDater(_source(), ["dob"], "%Y-%m-%d", "%d/%m/%Y")
    assert d.headers == ["name", "dob", "joined"]


def test_single_column_reformatted():
    d = CSVDater(_source(), ["dob"], "%Y-%m-%d", "%d/%m/%Y")
    result = list(d.rows())
    assert result[0]["dob"] == "15/04/1990"
    assert result[1]["dob"] == "03/11/1985"


def test_untouched_columns_preserved():
    d = CSVDater(_source(), ["dob"], "%Y-%m-%d", "%d/%m/%Y")
    result = list(d.rows())
    assert result[0]["name"] == "Alice"
    assert result[0]["joined"] == "2020-01-10"  # not reformatted


def test_multiple_columns_reformatted():
    d = CSVDater(_source(), ["dob", "joined"], "%Y-%m-%d", "%d/%m/%Y")
    result = list(d.rows())
    assert result[0]["dob"] == "15/04/1990"
    assert result[0]["joined"] == "10/01/2020"


def test_row_count_forwarded():
    d = CSVDater(_source(), ["dob"], "%Y-%m-%d", "%d/%m/%Y")
    assert d.row_count == 3


def test_errors_raise_on_bad_date():
    src = _FakeSource(
        headers=["name", "dob"],
        rows=[{"name": "X", "dob": "not-a-date"}],
    )
    d = CSVDater(src, ["dob"], "%Y-%m-%d", "%d/%m/%Y", errors="raise")
    with pytest.raises(ValueError):
        list(d.rows())


def test_errors_ignore_leaves_original():
    src = _FakeSource(
        headers=["name", "dob"],
        rows=[{"name": "X", "dob": "not-a-date"}],
    )
    d = CSVDater(src, ["dob"], "%Y-%m-%d", "%d/%m/%Y", errors="ignore")
    result = list(d.rows())
    assert result[0]["dob"] == "not-a-date"


def test_invalid_errors_arg():
    with pytest.raises(ValueError, match="errors must be"):
        CSVDater(_source(), ["dob"], "%Y-%m-%d", "%d/%m/%Y", errors="bad")


def test_all_rows_returned():
    d = CSVDater(_source(), ["dob"], "%Y-%m-%d", "%d/%m/%Y")
    result = list(d.rows())
    assert len(result) == 3
