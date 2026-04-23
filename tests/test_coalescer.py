"""Tests for CSVCoalescer."""
import pytest
from csvwrangler.coalescer import CSVCoalescer


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
        ["id", "primary", "secondary", "tertiary"],
        [
            ["1", "Alice",  "",       ""],
            ["2", "",       "Bob",    ""],
            ["3", "",       "",       "Carol"],
            ["4", "",       "",       ""],
            ["5", "Dave",   "Eve",    "Frank"],
        ],
    )


# ---------------------------------------------------------------------------
# headers
# ---------------------------------------------------------------------------

def test_headers_appends_new_target():
    c = CSVCoalescer(_source(), ["primary", "secondary", "tertiary"], "name")
    assert c.headers == ["id", "primary", "secondary", "tertiary", "name"]


def test_headers_no_duplicate_when_target_exists():
    c = CSVCoalescer(_source(), ["secondary", "tertiary"], "primary")
    assert c.headers == ["id", "primary", "secondary", "tertiary"]


# ---------------------------------------------------------------------------
# coalesce logic
# ---------------------------------------------------------------------------

def test_uses_first_non_empty():
    c = CSVCoalescer(_source(), ["primary", "secondary", "tertiary"], "name")
    result = list(c.rows())
    assert result[0]["name"] == "Alice"
    assert result[1]["name"] == "Bob"
    assert result[2]["name"] == "Carol"


def test_empty_string_when_all_empty():
    c = CSVCoalescer(_source(), ["primary", "secondary", "tertiary"], "name")
    result = list(c.rows())
    assert result[3]["name"] == ""


def test_first_column_wins_when_multiple_non_empty():
    c = CSVCoalescer(_source(), ["primary", "secondary", "tertiary"], "name")
    result = list(c.rows())
    assert result[4]["name"] == "Dave"


def test_overwrites_existing_target_column():
    c = CSVCoalescer(_source(), ["secondary", "tertiary"], "primary")
    result = list(c.rows())
    # row 0: primary was "Alice" but secondary="" tertiary="" → coalesced=""
    # row 1: secondary="Bob" → coalesced="Bob"
    assert result[0]["primary"] == ""
    assert result[1]["primary"] == "Bob"


# ---------------------------------------------------------------------------
# row_count
# ---------------------------------------------------------------------------

def test_row_count():
    c = CSVCoalescer(_source(), ["primary", "secondary"], "name")
    assert c.row_count == 5


# ---------------------------------------------------------------------------
# custom empty_values
# ---------------------------------------------------------------------------

def test_custom_empty_values():
    src = _FakeSource(
        ["a", "b"],
        [["N/A", "real"], ["good", "N/A"]],
    )
    c = CSVCoalescer(src, ["a", "b"], "out", empty_values={"N/A", "", None})
    result = list(c.rows())
    assert result[0]["out"] == "real"
    assert result[1]["out"] == "good"


# ---------------------------------------------------------------------------
# validation errors
# ---------------------------------------------------------------------------

def test_raises_on_empty_columns():
    with pytest.raises(ValueError, match="columns must not be empty"):
        CSVCoalescer(_source(), [], "name")


def test_raises_on_missing_column():
    with pytest.raises(ValueError, match="'missing'"):
        CSVCoalescer(_source(), ["primary", "missing"], "name")
