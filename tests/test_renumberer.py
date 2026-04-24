"""Tests for CSVRenumberer."""

import pytest
from csvwrangler.renumberer import CSVRenumberer


class _FakeSource:
    def __init__(self, headers, rows):
        self._headers = headers
        self._rows = rows

    @property
    def headers(self):
        return list(self._headers)

    def rows(self):
        yield from self._rows


def _source():
    return _FakeSource(
        headers=["id", "name", "score"],
        rows=[
            {"id": "5", "name": "Alice", "score": "90"},
            {"id": "10", "name": "Bob", "score": "85"},
            {"id": "99", "name": "Carol", "score": "78"},
        ],
    )


def test_headers_unchanged():
    r = CSVRenumberer(_source(), "id")
    assert r.headers == ["id", "name", "score"]


def test_default_start_and_step():
    r = CSVRenumberer(_source(), "id")
    ids = [row["id"] for row in r.rows()]
    assert ids == [1, 2, 3]


def test_custom_start():
    r = CSVRenumberer(_source(), "id", start=100)
    ids = [row["id"] for row in r.rows()]
    assert ids == [100, 101, 102]


def test_custom_step():
    r = CSVRenumberer(_source(), "id", start=0, step=10)
    ids = [row["id"] for row in r.rows()]
    assert ids == [0, 10, 20]


def test_negative_step():
    r = CSVRenumberer(_source(), "id", start=10, step=-1)
    ids = [row["id"] for row in r.rows()]
    assert ids == [10, 9, 8]


def test_float_step():
    r = CSVRenumberer(_source(), "score", start=1.0, step=0.5)
    scores = [row["score"] for row in r.rows()]
    assert scores == pytest.approx([1.0, 1.5, 2.0])


def test_other_columns_preserved():
    r = CSVRenumberer(_source(), "id")
    names = [row["name"] for row in r.rows()]
    assert names == ["Alice", "Bob", "Carol"]


def test_row_count():
    r = CSVRenumberer(_source(), "id")
    assert r.row_count == 3


def test_invalid_column_raises():
    with pytest.raises(ValueError, match="Column 'missing'"):
        CSVRenumberer(_source(), "missing")


def test_zero_step_raises():
    with pytest.raises(ValueError, match="step must not be zero"):
        CSVRenumberer(_source(), "id", step=0)


def test_rows_is_repeatable():
    r = CSVRenumberer(_source(), "id")
    first = [row["id"] for row in r.rows()]
    second = [row["id"] for row in r.rows()]
    assert first == second
