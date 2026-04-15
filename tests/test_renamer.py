"""Tests for CSVRenamer."""
import pytest
from csvwrangler.renamer import CSVRenamer


class _FakeSource:
    def __init__(self, headers, data):
        self._headers = headers
        self._data = data

    @property
    def headers(self):
        return list(self._headers)

    @property
    def rows(self):
        for row in self._data:
            yield dict(row)

    @property
    def row_count(self):
        return len(self._data)


def _source():
    return _FakeSource(
        headers=["id", "first_name", "score"],
        data=[
            {"id": "1", "first_name": "Alice", "score": "90"},
            {"id": "2", "first_name": "Bob", "score": "85"},
            {"id": "3", "first_name": "Carol", "score": "92"},
        ],
    )


# ---------------------------------------------------------------------------
# headers
# ---------------------------------------------------------------------------

def test_headers_unchanged_when_no_mapping():
    r = CSVRenamer(_source(), {})
    assert r.headers == ["id", "first_name", "score"]


def test_headers_single_rename():
    r = CSVRenamer(_source(), {"first_name": "name"})
    assert r.headers == ["id", "name", "score"]


def test_headers_multiple_renames():
    r = CSVRenamer(_source(), {"first_name": "name", "score": "points"})
    assert r.headers == ["id", "name", "points"]


def test_headers_returns_copy():
    r = CSVRenamer(_source(), {"id": "pk"})
    h1 = r.headers
    h1.append("extra")
    assert r.headers == ["pk", "first_name", "score"]


# ---------------------------------------------------------------------------
# rows
# ---------------------------------------------------------------------------

def test_rows_keys_are_renamed():
    r = CSVRenamer(_source(), {"first_name": "name"})
    rows = list(r.rows)
    for row in rows:
        assert "name" in row
        assert "first_name" not in row


def test_rows_values_preserved():
    r = CSVRenamer(_source(), {"first_name": "name", "score": "points"})
    rows = list(r.rows)
    assert rows[0] == {"id": "1", "name": "Alice", "points": "90"}
    assert rows[1] == {"id": "2", "name": "Bob", "points": "85"}


def test_rows_count_unchanged():
    r = CSVRenamer(_source(), {"id": "pk"})
    assert len(list(r.rows)) == 3


# ---------------------------------------------------------------------------
# row_count
# ---------------------------------------------------------------------------

def test_row_count_delegates_to_source():
    r = CSVRenamer(_source(), {"id": "pk"})
    assert r.row_count == 3


# ---------------------------------------------------------------------------
# validation
# ---------------------------------------------------------------------------

def test_unknown_column_raises_value_error():
    with pytest.raises(ValueError, match="not found in source"):
        CSVRenamer(_source(), {"nonexistent": "x"})


def test_non_dict_mapping_raises_type_error():
    with pytest.raises(TypeError, match="mapping must be a dict"):
        CSVRenamer(_source(), [("id", "pk")])
