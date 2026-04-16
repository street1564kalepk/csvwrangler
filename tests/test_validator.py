"""Tests for CSVValidator."""
import pytest
from csvwrangler.validator import CSVValidator


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
        ["name", "age", "email"],
        [
            ["Alice", "30", "alice@example.com"],
            ["Bob", "abc", "bob@example.com"],
            ["", "25", "carol@example.com"],
            ["Dave", "40", "not-an-email"],
        ],
    )


def _rules():
    return {
        "name": lambda v: len(v) > 0,
        "age": lambda v: v.isdigit(),
        "email": lambda v: "@" in v,
    }


def test_headers_drop_mode():
    v = CSVValidator(_source(), _rules(), mode="drop")
    assert v.headers == ["name", "age", "email"]


def test_headers_tag_mode():
    v = CSVValidator(_source(), _rules(), mode="tag")
    assert v.headers == ["name", "age", "email", "_errors"]


def test_drop_removes_invalid_rows():
    v = CSVValidator(_source(), _rules(), mode="drop")
    result = list(v.rows())
    assert len(result) == 1
    assert result[0]["name"] == "Alice"


def test_tag_keeps_all_rows():
    v = CSVValidator(_source(), _rules(), mode="tag")
    result = list(v.rows())
    assert len(result) == 4


def test_tag_empty_errors_for_valid():
    v = CSVValidator(_source(), _rules(), mode="tag")
    result = list(v.rows())
    assert result[0]["_errors"] == ""


def test_tag_errors_listed_for_invalid():
    v = CSVValidator(_source(), _rules(), mode="tag")
    result = list(v.rows())
    bob = result[1]
    assert "age" in bob["_errors"]
    carol = result[2]
    assert "name" in carol["_errors"]


def test_raise_mode_raises_on_invalid():
    v = CSVValidator(_source(), _rules(), mode="raise")
    with pytest.raises(ValueError, match="Validation failed"):
        list(v.rows())


def test_invalid_mode_raises():
    with pytest.raises(ValueError, match="mode must be"):
        CSVValidator(_source(), _rules(), mode="ignore")


def test_unknown_column_raises():
    with pytest.raises(ValueError, match="Unknown columns"):
        CSVValidator(_source(), {"nonexistent": lambda v: True})


def test_row_count_drop():
    v = CSVValidator(_source(), _rules(), mode="drop")
    assert v.row_count() == 1


def test_custom_tag_column():
    v = CSVValidator(_source(), _rules(), mode="tag", tag_column="_validation")
    assert "_validation" in v.headers
    result = list(v.rows())
    assert "_validation" in result[0]
