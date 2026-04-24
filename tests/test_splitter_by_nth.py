"""Tests for CSVNthSplitter."""

import pytest
from csvwrangler.splitter_by_nth import CSVNthSplitter


class _FakeSource:
    def __init__(self, headers, rows):
        self._headers = headers
        self._rows = rows

    @property
    def headers(self):
        return self._headers

    def rows(self):
        yield from self._rows


def _source():
    """Return a source with 6 rows numbered 1-6."""
    hdrs = ["id", "value"]
    data = [{"id": str(i), "value": str(i * 10)} for i in range(1, 7)]
    return _FakeSource(hdrs, data)


# ---------------------------------------------------------------------------
# Construction guards
# ---------------------------------------------------------------------------

def test_invalid_n_zero():
    with pytest.raises(ValueError):
        CSVNthSplitter(_source(), n=0)


def test_invalid_n_one():
    with pytest.raises(ValueError):
        CSVNthSplitter(_source(), n=1)


def test_invalid_n_negative():
    with pytest.raises(ValueError):
        CSVNthSplitter(_source(), n=-3)


def test_invalid_n_float():
    with pytest.raises(ValueError):
        CSVNthSplitter(_source(), n=2.0)  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# Headers
# ---------------------------------------------------------------------------

def test_headers_unchanged():
    sp = CSVNthSplitter(_source(), n=2)
    assert sp.headers == ["id", "value"]


# ---------------------------------------------------------------------------
# Splitting behaviour – n=2 (every 2nd row)
# ---------------------------------------------------------------------------

def test_nth_count_n2():
    sp = CSVNthSplitter(_source(), n=2)
    # rows 2, 4, 6 → 3 nth rows
    assert sp.nth_count == 3


def test_rest_count_n2():
    sp = CSVNthSplitter(_source(), n=2)
    # rows 1, 3, 5 → 3 rest rows
    assert sp.rest_count == 3


def test_total_row_count_n2():
    sp = CSVNthSplitter(_source(), n=2)
    assert sp.row_count == 6


def test_nth_row_ids_n2():
    sp = CSVNthSplitter(_source(), n=2)
    ids = [r["id"] for r in sp.nth_rows()]
    assert ids == ["2", "4", "6"]


def test_rest_row_ids_n2():
    sp = CSVNthSplitter(_source(), n=2)
    ids = [r["id"] for r in sp.rest_rows()]
    assert ids == ["1", "3", "5"]


# ---------------------------------------------------------------------------
# Splitting behaviour – n=3 (every 3rd row)
# ---------------------------------------------------------------------------

def test_nth_count_n3():
    sp = CSVNthSplitter(_source(), n=3)
    # rows 3, 6 → 2 nth rows
    assert sp.nth_count == 2


def test_rest_count_n3():
    sp = CSVNthSplitter(_source(), n=3)
    assert sp.rest_count == 4


def test_nth_row_ids_n3():
    sp = CSVNthSplitter(_source(), n=3)
    ids = [r["id"] for r in sp.nth_rows()]
    assert ids == ["3", "6"]


# ---------------------------------------------------------------------------
# Idempotency – calling rows multiple times
# ---------------------------------------------------------------------------

def test_nth_rows_idempotent():
    sp = CSVNthSplitter(_source(), n=2)
    first = list(sp.nth_rows())
    second = list(sp.nth_rows())
    assert first == second


def test_rest_rows_idempotent():
    sp = CSVNthSplitter(_source(), n=2)
    first = list(sp.rest_rows())
    second = list(sp.rest_rows())
    assert first == second
