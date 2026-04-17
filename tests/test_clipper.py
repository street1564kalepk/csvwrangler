"""Tests for CSVClipper."""
import pytest
from csvwrangler.clipper import CSVClipper


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
        ["name", "score", "age"],
        [
            {"name": "Alice", "score": "95", "age": "30"},
            {"name": "Bob",   "score": "42", "age": "17"},
            {"name": "Carol", "score": "105", "age": "200"},
            {"name": "Dave",  "score": "-10", "age": "25"},
            {"name": "Eve",   "score": "bad", "age": "22"},
        ],
    )


# ---------------------------------------------------------------------------
def test_headers_unchanged():
    c = CSVClipper(_source(), {"score": (0, 100)})
    assert c.headers == ["name", "score", "age"]


def test_clamp_upper_bound():
    c = CSVClipper(_source(), {"score": (0, 100)})
    rows = list(c.rows())
    assert rows[2]["score"] == 100   # 105 clamped to 100


def test_clamp_lower_bound():
    c = CSVClipper(_source(), {"score": (0, 100)})
    rows = list(c.rows())
    assert rows[3]["score"] == 0     # -10 clamped to 0


def test_value_within_bounds_unchanged():
    c = CSVClipper(_source(), {"score": (0, 100)})
    rows = list(c.rows())
    assert rows[0]["score"] == 95
    assert rows[1]["score"] == 42


def test_non_numeric_value_left_alone():
    c = CSVClipper(_source(), {"score": (0, 100)})
    rows = list(c.rows())
    assert rows[4]["score"] == "bad"


def test_no_upper_bound():
    c = CSVClipper(_source(), {"age": (18, None)})
    rows = list(c.rows())
    assert rows[1]["age"] == 18     # 17 -> 18
    assert rows[2]["age"] == 200    # 200 left alone (no upper)


def test_no_lower_bound():
    c = CSVClipper(_source(), {"score": (None, 100)})
    rows = list(c.rows())
    assert rows[2]["score"] == 100
    assert rows[3]["score"] == -10  # no lower bound, kept as-is


def test_multiple_columns():
    c = CSVClipper(_source(), {"score": (0, 100), "age": (18, 65)})
    rows = list(c.rows())
    assert rows[1]["age"] == 18
    assert rows[2]["age"] == 65
    assert rows[2]["score"] == 100


def test_row_count_unchanged():
    c = CSVClipper(_source(), {"score": (0, 100)})
    assert c.row_count == 5


def test_unknown_column_raises():
    with pytest.raises(ValueError, match="unknown columns"):
        CSVClipper(_source(), {"nonexistent": (0, 100)})
