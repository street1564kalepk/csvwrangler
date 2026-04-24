"""Tests for CSVRegexSplitter."""
from __future__ import annotations

import pytest

from csvwrangler.splitter_by_regex import CSVRegexSplitter


class _FakeSource:
    def __init__(self, headers, data):
        self._headers = headers
        self._data = data

    @property
    def headers(self):
        return list(self._headers)

    def rows(self):
        yield from self._data


def _source():
    return _FakeSource(
        headers=["name", "email", "dept"],
        data=[
            {"name": "Alice",   "email": "alice@example.com",  "dept": "Engineering"},
            {"name": "Bob",     "email": "bob@corp.org",        "dept": "Marketing"},
            {"name": "Carol",   "email": "carol@example.com",  "dept": "Engineering"},
            {"name": "Dave",    "email": "dave@nowhere.net",   "dept": "HR"},
            {"name": "Eve",     "email": "eve@corp.org",        "dept": "Engineering"},
            {"name": "Frank",   "email": "frank@example.com",  "dept": "Finance"},
        ],
    )


# ---------------------------------------------------------------------------
def test_headers_unchanged():
    splitter = CSVRegexSplitter(_source(), "email", {"example": r"@example\.com"})
    assert splitter.headers == ["name", "email", "dept"]


def test_invalid_column_raises():
    with pytest.raises(ValueError, match="not found"):
        CSVRegexSplitter(_source(), "nonexistent", {"x": r"."})


def test_group_keys_contains_named_and_unmatched():
    splitter = CSVRegexSplitter(
        _source(), "email",
        {"example": r"@example\.com", "corp": r"@corp\.org"}
    )
    keys = splitter.group_keys
    assert "example" in keys
    assert "corp" in keys
    assert "__unmatched__" in keys


def test_group_count():
    splitter = CSVRegexSplitter(
        _source(), "email",
        {"example": r"@example\.com", "corp": r"@corp\.org"}
    )
    assert splitter.group_count() == 3  # example, corp, unmatched


def test_rows_example_group():
    splitter = CSVRegexSplitter(_source(), "email", {"example": r"@example\.com"})
    names = [r["name"] for r in splitter.rows("example")]
    assert sorted(names) == ["Alice", "Carol", "Frank"]


def test_rows_corp_group():
    splitter = CSVRegexSplitter(
        _source(), "email",
        {"example": r"@example\.com", "corp": r"@corp\.org"}
    )
    names = [r["name"] for r in splitter.rows("corp")]
    assert sorted(names) == ["Bob", "Eve"]


def test_unmatched_group():
    splitter = CSVRegexSplitter(
        _source(), "email",
        {"example": r"@example\.com", "corp": r"@corp\.org"}
    )
    names = [r["name"] for r in splitter.rows("__unmatched__")]
    assert names == ["Dave"]


def test_row_count_matches_rows():
    splitter = CSVRegexSplitter(_source(), "email", {"example": r"@example\.com"})
    assert splitter.row_count("example") == 3


def test_first_match_wins():
    """A row matching multiple patterns should only appear in the first."""
    splitter = CSVRegexSplitter(
        _source(), "email",
        {"dot_com": r"\.com$", "example": r"@example\.com"}
    )
    dot_com_names = [r["name"] for r in splitter.rows("dot_com")]
    example_names = [r["name"] for r in splitter.rows("example")]
    # All .com addresses go into dot_com; example group should be empty
    assert "Alice" in dot_com_names
    assert "Alice" not in example_names


def test_empty_group_returns_no_rows():
    splitter = CSVRegexSplitter(_source(), "email", {"nothing": r"@nonexistent\.xyz"})
    assert list(splitter.rows("nothing")) == []
    assert splitter.row_count("nothing") == 0
