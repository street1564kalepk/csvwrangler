"""Tests for CSVHeaderSplitter."""

from __future__ import annotations

import pytest

from csvwrangler.splitter_by_header import CSVHeaderSplitter


# ---------------------------------------------------------------------------
# Fake source
# ---------------------------------------------------------------------------


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
    headers = ["id", "sales_q1", "sales_q2", "cost_q1", "cost_q2"]
    data = [
        {"id": "1", "sales_q1": "100", "sales_q2": "200", "cost_q1": "40", "cost_q2": "50"},
        {"id": "2", "sales_q1": "300", "sales_q2": "400", "cost_q1": "60", "cost_q2": "70"},
    ]
    return _FakeSource(headers, data)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_group_keys_detected():
    splitter = CSVHeaderSplitter(_source())
    keys = splitter.group_keys
    assert set(keys) == {"", "sales", "cost"}


def test_group_count():
    splitter = CSVHeaderSplitter(_source())
    assert splitter.group_count == 3


def test_group_headers_sales():
    splitter = CSVHeaderSplitter(_source())
    assert splitter.group_headers("sales") == ["sales_q1", "sales_q2"]


def test_group_headers_cost():
    splitter = CSVHeaderSplitter(_source())
    assert splitter.group_headers("cost") == ["cost_q1", "cost_q2"]


def test_group_headers_no_prefix():
    splitter = CSVHeaderSplitter(_source())
    assert splitter.group_headers("") == ["id"]


def test_rows_projected_correctly():
    splitter = CSVHeaderSplitter(_source())
    result = list(splitter.rows("sales"))
    assert result[0] == {"sales_q1": "100", "sales_q2": "200"}
    assert result[1] == {"sales_q1": "300", "sales_q2": "400"}


def test_rows_no_prefix_group():
    splitter = CSVHeaderSplitter(_source())
    result = list(splitter.rows(""))
    assert [r["id"] for r in result] == ["1", "2"]


def test_headers_property_unchanged():
    splitter = CSVHeaderSplitter(_source())
    assert splitter.headers == ["id", "sales_q1", "sales_q2", "cost_q1", "cost_q2"]


def test_unknown_group_raises_key_error():
    splitter = CSVHeaderSplitter(_source())
    with pytest.raises(KeyError):
        splitter.group_headers("nonexistent")


def test_custom_separator():
    headers = ["a.x", "a.y", "b.z"]
    data = [{"a.x": "1", "a.y": "2", "b.z": "3"}]
    src = _FakeSource(headers, data)
    splitter = CSVHeaderSplitter(src, separator=".")
    assert set(splitter.group_keys) == {"a", "b"}
    assert splitter.group_headers("a") == ["a.x", "a.y"]


def test_empty_separator_raises():
    with pytest.raises(ValueError):
        CSVHeaderSplitter(_source(), separator="")
