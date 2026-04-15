"""Tests for CSVJoiner."""

import pytest
from csvwrangler.joiner import CSVJoiner


class _FakeSource:
    def __init__(self, headers, data):
        self._headers = headers
        self._data = data

    @property
    def headers(self):
        return self._headers

    def rows(self):
        return iter(self._data)


def _left():
    return _FakeSource(
        ["id", "name"],
        [
            {"id": "1", "name": "Alice"},
            {"id": "2", "name": "Bob"},
            {"id": "3", "name": "Carol"},
        ],
    )


def _right():
    return _FakeSource(
        ["id", "city"],
        [
            {"id": "1", "city": "London"},
            {"id": "2", "city": "Paris"},
        ],
    )


# --- header tests ---

def test_joiner_headers_merged():
    j = CSVJoiner(_left(), _right(), on="id")
    assert j.headers == ["id", "name", "city"]


def test_joiner_headers_no_duplicate_key():
    j = CSVJoiner(_left(), _right(), on="id")
    assert j.headers.count("id") == 1


# --- inner join ---

def test_inner_join_row_count():
    j = CSVJoiner(_left(), _right(), on="id", how="inner")
    result = list(j.rows())
    assert len(result) == 2  # Carol has no match


def test_inner_join_values():
    j = CSVJoiner(_left(), _right(), on="id", how="inner")
    result = list(j.rows())
    assert result[0] == {"id": "1", "name": "Alice", "city": "London"}
    assert result[1] == {"id": "2", "name": "Bob", "city": "Paris"}


def test_inner_join_excludes_unmatched():
    j = CSVJoiner(_left(), _right(), on="id", how="inner")
    ids = [r["id"] for r in j.rows()]
    assert "3" not in ids


# --- left join ---

def test_left_join_row_count():
    j = CSVJoiner(_left(), _right(), on="id", how="left")
    result = list(j.rows())
    assert len(result) == 3  # all left rows preserved


def test_left_join_unmatched_empty_string():
    j = CSVJoiner(_left(), _right(), on="id", how="left")
    result = list(j.rows())
    carol = next(r for r in result if r["id"] == "3")
    assert carol["city"] == ""


# --- error cases ---

def test_invalid_join_type_raises():
    with pytest.raises(ValueError, match="Unsupported join type"):
        CSVJoiner(_left(), _right(), on="id", how="outer")


def test_missing_key_in_left_raises():
    with pytest.raises(KeyError, match="Join key 'missing'"):
        CSVJoiner(_left(), _right(), on="missing")


def test_missing_key_in_right_raises():
    bad_right = _FakeSource(["ref", "city"], [])
    with pytest.raises(KeyError, match="Join key 'id'"):
        CSVJoiner(_left(), bad_right, on="id")
