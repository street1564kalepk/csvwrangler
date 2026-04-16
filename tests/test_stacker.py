"""Tests for CSVStacker."""
import pytest
from csvwrangler.stacker import CSVStacker


class _FakeSource:
    def __init__(self, headers, data):
        self._headers = headers
        self._data = data

    @property
    def headers(self):
        return self._headers

    def rows(self):
        yield from self._data


def _source(headers, data):
    return _FakeSource(headers, data)


@pytest.fixture
def left():
    return _source(
        ["name", "age"],
        [{"name": "Alice", "age": "30"}, {"name": "Bob", "age": "25"}],
    )


@pytest.fixture
def right():
    return _source(
        ["name", "age"],
        [{"name": "Carol", "age": "28"}, {"name": "Dave", "age": "35"}],
    )


def test_headers_unchanged(left, right):
    stacker = CSVStacker(left, right)
    assert stacker.headers == ["name", "age"]


def test_row_count(left, right):
    stacker = CSVStacker(left, right)
    assert stacker.row_count == 4


def test_rows_combined(left, right):
    stacker = CSVStacker(left, right)
    result = list(stacker.rows())
    names = [r["name"] for r in result]
    assert names == ["Alice", "Bob", "Carol", "Dave"]


def test_single_source(left):
    stacker = CSVStacker(left)
    result = list(stacker.rows())
    assert len(result) == 2
    assert result[0]["name"] == "Alice"


def test_three_sources():
    s1 = _source(["x"], [{"x": "1"}])
    s2 = _source(["x"], [{"x": "2"}])
    s3 = _source(["x"], [{"x": "3"}])
    stacker = CSVStacker(s1, s2, s3)
    assert stacker.row_count == 3
    assert [r["x"] for r in stacker.rows()] == ["1", "2", "3"]


def test_mismatched_headers_raises(left):
    bad = _source(["name", "city"], [{"name": "X", "city": "Y"}])
    with pytest.raises(ValueError, match="same headers"):
        CSVStacker(left, bad)


def test_no_sources_raises():
    with pytest.raises(ValueError, match="at least one source"):
        CSVStacker()


def test_missing_column_filled_with_empty():
    """If a row dict is missing a column it should default to empty string."""
    s1 = _source(["a", "b"], [{"a": "1"}])  # missing 'b'
    s2 = _source(["a", "b"], [{"a": "2", "b": "3"}])
    stacker = CSVStacker(s1, s2)
    rows = list(stacker.rows())
    assert rows[0]["b"] == ""
    assert rows[1]["b"] == "3"
