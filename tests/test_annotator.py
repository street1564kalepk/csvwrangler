"""Tests for CSVAnnotator."""
import pytest
from csvwrangler.annotator import CSVAnnotator


class _FakeSource:
    def __init__(self, headers, rows):
        self._headers = headers
        self._rows = rows

    @property
    def headers(self):
        return list(self._headers)

    def rows(self):
        yield from (dict(r) for r in self._rows)


def _source():
    return _FakeSource(
        ["name", "city"],
        [
            {"name": "Alice", "city": "London"},
            {"name": "Bob",   "city": "Paris"},
            {"name": "Carol", "city": "Berlin"},
        ],
    )


# ── headers ───────────────────────────────────────────────────────────

def test_headers_appends_new_column():
    ann = CSVAnnotator(_source(), "source", "manual")
    assert ann.headers == ["name", "city", "source"]


def test_headers_overwrite_preserves_order():
    ann = CSVAnnotator(_source(), "city", "UNKNOWN", overwrite=True)
    assert ann.headers == ["name", "city"]


# ── rows ──────────────────────────────────────────────────────────────

def test_rows_adds_constant_value():
    ann = CSVAnnotator(_source(), "source", "manual")
    result = list(ann.rows())
    assert all(r["source"] == "manual" for r in result)


def test_rows_preserves_existing_data():
    ann = CSVAnnotator(_source(), "source", "x")
    result = list(ann.rows())
    assert result[0]["name"] == "Alice"
    assert result[1]["city"] == "Paris"


def test_rows_count_unchanged():
    ann = CSVAnnotator(_source(), "source", "x")
    assert len(list(ann.rows())) == 3


def test_overwrite_replaces_value():
    ann = CSVAnnotator(_source(), "city", "UNKNOWN", overwrite=True)
    result = list(ann.rows())
    assert all(r["city"] == "UNKNOWN" for r in result)


def test_value_coerced_to_string():
    ann = CSVAnnotator(_source(), "score", 42)
    result = list(ann.rows())
    assert result[0]["score"] == "42"


# ── row_count ─────────────────────────────────────────────────────────

def test_row_count():
    ann = CSVAnnotator(_source(), "tag", "v1")
    assert ann.row_count() == 3


# ── validation ────────────────────────────────────────────────────────

def test_raises_on_empty_column_name():
    with pytest.raises(ValueError, match="column name"):
        CSVAnnotator(_source(), "", "x")


def test_raises_on_duplicate_column_without_overwrite():
    with pytest.raises(ValueError, match="already exists"):
        CSVAnnotator(_source(), "city", "x")


def test_no_raise_on_duplicate_with_overwrite():
    ann = CSVAnnotator(_source(), "city", "x", overwrite=True)
    assert ann is not None
