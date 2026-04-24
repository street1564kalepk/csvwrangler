"""Tests for CSVMapper."""

from __future__ import annotations

import pytest

from csvwrangler.mapper import CSVMapper


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

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
        ["name", "dept", "status"],
        [
            {"name": "Alice", "dept": "eng", "status": "A"},
            {"name": "Bob",   "dept": "hr",  "status": "I"},
            {"name": "Carol", "dept": "eng", "status": "A"},
            {"name": "Dave",  "dept": "fin", "status": "X"},
        ],
    )


# ---------------------------------------------------------------------------
# construction guards
# ---------------------------------------------------------------------------

def test_empty_columns_raises():
    with pytest.raises(ValueError):
        CSVMapper(_source(), [], {"A": "Active"})


def test_non_dict_mapping_raises():
    with pytest.raises(TypeError):
        CSVMapper(_source(), ["status"], [("A", "Active")])


# ---------------------------------------------------------------------------
# headers
# ---------------------------------------------------------------------------

def test_headers_unchanged():
    m = CSVMapper(_source(), ["status"], {"A": "Active"})
    assert m.headers == ["name", "dept", "status"]


# ---------------------------------------------------------------------------
# mapping behaviour
# ---------------------------------------------------------------------------

def test_mapped_values_replaced():
    mapping = {"A": "Active", "I": "Inactive"}
    m = CSVMapper(_source(), ["status"], mapping)
    result = list(m.rows())
    assert result[0]["status"] == "Active"
    assert result[1]["status"] == "Inactive"


def test_unmapped_values_unchanged_without_default():
    mapping = {"A": "Active"}
    m = CSVMapper(_source(), ["status"], mapping)
    result = list(m.rows())
    # "X" has no mapping entry and no default
    assert result[3]["status"] == "X"


def test_unmapped_values_replaced_with_default():
    mapping = {"A": "Active", "I": "Inactive"}
    m = CSVMapper(_source(), ["status"], mapping, default="Unknown")
    result = list(m.rows())
    assert result[3]["status"] == "Unknown"


def test_multiple_columns_mapped():
    mapping = {"eng": "Engineering", "hr": "Human Resources", "fin": "Finance"}
    m = CSVMapper(_source(), ["dept"], mapping)
    result = list(m.rows())
    assert result[0]["dept"] == "Engineering"
    assert result[1]["dept"] == "Human Resources"
    assert result[2]["dept"] == "Engineering"
    assert result[3]["dept"] == "Finance"


def test_non_existent_column_is_silently_skipped():
    m = CSVMapper(_source(), ["nonexistent"], {"A": "Active"})
    result = list(m.rows())
    # rows should be yielded unchanged
    assert result[0]["name"] == "Alice"


def test_row_count():
    m = CSVMapper(_source(), ["status"], {"A": "Active"})
    assert m.row_count == 4


def test_other_columns_untouched():
    m = CSVMapper(_source(), ["status"], {"A": "Active"})
    result = list(m.rows())
    assert result[0]["name"] == "Alice"
    assert result[0]["dept"] == "eng"
