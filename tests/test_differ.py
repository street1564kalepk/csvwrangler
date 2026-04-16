"""Tests for CSVDiffer."""
import pytest
from csvwrangler.differ import CSVDiffer


class _FakeSource:
    def __init__(self, headers, data):
        self._headers = headers
        self._data = data

    @property
    def headers(self):
        return list(self._headers)

    def rows(self):
        yield from (dict(r) for r in self._data)


def _left():
    return _FakeSource(
        ["id", "name", "city"],
        [
            {"id": "1", "name": "Alice", "city": "NY"},
            {"id": "2", "name": "Bob", "city": "LA"},
            {"id": "3", "name": "Carol", "city": "SF"},
        ],
    )


def _right():
    return _FakeSource(
        ["id", "name", "city"],
        [
            {"id": "1", "name": "Alice", "city": "NY"},   # unchanged
            {"id": "2", "name": "Bob", "city": "Chicago"}, # changed
            {"id": "4", "name": "Dave", "city": "Boston"}, # added
        ],
    )


def test_headers_include_diff_tag():
    d = CSVDiffer(_left(), _right(), key="id")
    assert d.headers[0] == "_diff"
    assert "id" in d.headers
    assert "name" in d.headers


def test_detect_removed():
    d = CSVDiffer(_left(), _right(), key="id", mode="removed")
    result = list(d.rows())
    assert len(result) == 1
    assert result[0]["_diff"] == "removed"
    assert result[0]["id"] == "3"


def test_detect_added():
    d = CSVDiffer(_left(), _right(), key="id", mode="added")
    result = list(d.rows())
    assert len(result) == 1
    assert result[0]["_diff"] == "added"
    assert result[0]["id"] == "4"


def test_detect_changed():
    d = CSVDiffer(_left(), _right(), key="id", mode="changed")
    result = list(d.rows())
    assert len(result) == 1
    assert result[0]["_diff"] == "changed"
    assert result[0]["city"] == "Chicago"


def test_all_mode_excludes_unchanged():
    d = CSVDiffer(_left(), _right(), key="id", mode="all")
    result = list(d.rows())
    tags = {r["_diff"] for r in result}
    assert "unchanged" not in tags
    assert len(result) == 3


def test_row_count():
    d = CSVDiffer(_left(), _right(), key="id", mode="all")
    assert d.row_count == 3


def test_invalid_mode_raises():
    with pytest.raises(ValueError, match="mode must be"):
        CSVDiffer(_left(), _right(), key="id", mode="invalid")


def test_missing_key_raises():
    with pytest.raises(ValueError, match="key 'x' not in left"):
        CSVDiffer(_left(), _right(), key="x")
