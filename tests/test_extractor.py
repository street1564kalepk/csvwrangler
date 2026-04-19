"""Tests for CSVExtractor."""
from __future__ import annotations
import pytest
from csvwrangler.extractor import CSVExtractor


class _FakeSource:
    def __init__(self, headers, data):
        self._headers = headers
        self._data = data

    @property
    def headers(self):
        return list(self._headers)

    def rows(self):
        yield from (dict(r) for r in self._data)


def _source():
    return _FakeSource(
        ["id", "email", "score"],
        [
            {"id": "1", "email": "alice@example.com", "score": "42"},
            {"id": "2", "email": "bob@test.org", "score": "17"},
            {"id": "3", "email": "charlie", "score": "99"},
        ],
    )


def test_headers_include_dest_col():
    ex = CSVExtractor(_source(), "email", r"@(.+)$", "domain")
    assert ex.headers == ["id", "email", "score", "domain"]


def test_extract_capture_group():
    ex = CSVExtractor(_source(), "email", r"@(.+)$", "domain")
    result = list(ex.rows())
    assert result[0]["domain"] == "example.com"
    assert result[1]["domain"] == "test.org"


def test_no_match_uses_default():
    ex = CSVExtractor(_source(), "email", r"@(.+)$", "domain", default="unknown")
    result = list(ex.rows())
    assert result[2]["domain"] == "unknown"


def test_no_match_empty_string_default():
    ex = CSVExtractor(_source(), "email", r"@(.+)$", "domain")
    result = list(ex.rows())
    assert result[2]["domain"] == ""


def test_full_match_when_no_capture_group():
    ex = CSVExtractor(_source(), "score", r"\d+", "digits")
    result = list(ex.rows())
    assert result[0]["digits"] == "42"
    assert result[1]["digits"] == "17"


def test_original_columns_preserved():
    ex = CSVExtractor(_source(), "email", r"@(.+)$", "domain")
    result = list(ex.rows())
    assert result[0]["email"] == "alice@example.com"
    assert result[0]["id"] == "1"


def test_row_count():
    ex = CSVExtractor(_source(), "email", r"@(.+)$", "domain")
    assert ex.row_count == 3


def test_missing_source_col_raises():
    with pytest.raises(ValueError, match="not found"):
        CSVExtractor(_source(), "nonexistent", r".", "out")


def test_dest_col_already_exists_raises():
    with pytest.raises(ValueError, match="already exists"):
        CSVExtractor(_source(), "email", r".", "score")
