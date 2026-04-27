"""Tests for CSVBooleanSplitter."""

import pytest
from csvwrangler.splitter_by_boolean import CSVBooleanSplitter


class _FakeSource:
    def __init__(self, headers, rows):
        self._headers = headers
        self._rows = rows

    @property
    def headers(self):
        return self._headers

    @property
    def rows(self):
        return iter(self._rows)


def _source():
    return _FakeSource(
        headers=["name", "active", "score"],
        rows=[
            {"name": "Alice", "active": "true", "score": "90"},
            {"name": "Bob", "active": "false", "score": "70"},
            {"name": "Carol", "active": "1", "score": "85"},
            {"name": "Dave", "active": "0", "score": "55"},
            {"name": "Eve", "active": "yes", "score": "95"},
            {"name": "Frank", "active": "no", "score": "60"},
            {"name": "Grace", "active": "", "score": "40"},
        ],
    )


def test_headers_unchanged():
    splitter = CSVBooleanSplitter(_source(), "active")
    assert splitter.headers == ["name", "active", "score"]


def test_true_rows_count():
    splitter = CSVBooleanSplitter(_source(), "active")
    assert splitter.true_count == 3  # true, 1, yes


def test_false_rows_count():
    splitter = CSVBooleanSplitter(_source(), "active")
    assert splitter.false_count == 4  # false, 0, no, ""


def test_true_rows_names():
    splitter = CSVBooleanSplitter(_source(), "active")
    names = [r["name"] for r in splitter.true_rows]
    assert names == ["Alice", "Carol", "Eve"]


def test_false_rows_names():
    splitter = CSVBooleanSplitter(_source(), "active")
    names = [r["name"] for r in splitter.false_rows]
    assert names == ["Bob", "Dave", "Frank", "Grace"]


def test_missing_column_raises():
    splitter = CSVBooleanSplitter(_source(), "nonexistent")
    with pytest.raises(KeyError, match="nonexistent"):
        _ = splitter.true_rows


def test_strict_mode_raises_on_unknown_value():
    src = _FakeSource(
        headers=["name", "flag"],
        rows=[{"name": "Alice", "flag": "maybe"}],
    )
    splitter = CSVBooleanSplitter(src, "flag", strict=True)
    with pytest.raises(ValueError, match="maybe"):
        _ = splitter.true_rows


def test_strict_mode_passes_on_known_values():
    src = _FakeSource(
        headers=["name", "flag"],
        rows=[
            {"name": "Alice", "flag": "true"},
            {"name": "Bob", "flag": "false"},
        ],
    )
    splitter = CSVBooleanSplitter(src, "flag", strict=True)
    assert splitter.true_count == 1
    assert splitter.false_count == 1


def test_result_is_cached():
    splitter = CSVBooleanSplitter(_source(), "active")
    first = splitter.true_rows
    second = splitter.true_rows
    assert first is second
