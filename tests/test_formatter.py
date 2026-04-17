"""Tests for CSVFormatter."""
import pytest
from csvwrangler.formatter import CSVFormatter


class _FakeSource:
    def __init__(self, headers, data):
        self._headers = headers
        self._data = data

    @property
    def headers(self):
        return self._headers

    def rows(self):
        for row in self._data:
            yield dict(zip(self._headers, row))


def _source():
    return _FakeSource(
        ["first", "last", "age", "score"],
        [
            ["Alice", "Smith", "30", "95.5"],
            ["Bob", "Jones", "25", "80.0"],
        ],
    )


def test_headers_unchanged():
    f = CSVFormatter(_source(), {"score": "{value}%"})
    assert f.headers == ["first", "last", "age", "score"]


def test_simple_value_format():
    f = CSVFormatter(_source(), {"score": "{value}%"})
    result = list(f.rows())
    assert result[0]["score"] == "95.5%"
    assert result[1]["score"] == "80.0%"


def test_cross_column_format():
    f = CSVFormatter(_source(), {"first": "{first} {last}"})
    result = list(f.rows())
    assert result[0]["first"] == "Alice Smith"
    assert result[1]["first"] == "Bob Jones"


def test_multiple_columns():
    f = CSVFormatter(_source(), {"first": "{first} {last}", "score": "[{value}]"})
    result = list(f.rows())
    assert result[0]["first"] == "Alice Smith"
    assert result[0]["score"] == "[95.5]"


def test_untouched_columns_preserved():
    f = CSVFormatter(_source(), {"score": "{value}!"})
    result = list(f.rows())
    assert result[0]["first"] == "Alice"
    assert result[0]["last"] == "Smith"
    assert result[0]["age"] == "30"


def test_unknown_column_raises():
    with pytest.raises(ValueError, match="Unknown columns"):
        CSVFormatter(_source(), {"nonexistent": "{value}"})


def test_row_count():
    f = CSVFormatter(_source(), {"age": "Age: {value}"})
    assert f.row_count == 2


def test_bad_template_raises():
    f = CSVFormatter(_source(), {"score": "{missing_col}"})
    with pytest.raises(ValueError, match="Format error"):
        list(f.rows())
