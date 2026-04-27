"""Tests for CSVLengthSplitter."""

import pytest
from csvwrangler.splitter_by_length import CSVLengthSplitter


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
        headers=["name", "city"],
        rows=[
            {"name": "Al", "city": "NY"},          # name len=2  -> short (<5)
            {"name": "Bob", "city": "LA"},          # name len=3  -> short
            {"name": "Clara", "city": "Austin"},    # name len=5  -> medium (5-9)
            {"name": "Daniel", "city": "Denver"},   # name len=6  -> medium
            {"name": "Elizabeth", "city": "Miami"}, # name len=9  -> medium
            {"name": "Christopher", "city": "Chicago"},  # name len=11 -> long (>=10)
            {"name": "Bartholomew", "city": "Boston"},   # name len=11 -> long
        ],
    )


def test_headers_unchanged():
    splitter = CSVLengthSplitter(_source(), column="name")
    assert splitter.headers == ["name", "city"]


def test_short_threshold_validation():
    with pytest.raises(ValueError, match="short_threshold"):
        CSVLengthSplitter(_source(), column="name", short_threshold=10, long_threshold=5)


def test_equal_thresholds_raises():
    with pytest.raises(ValueError):
        CSVLengthSplitter(_source(), column="name", short_threshold=5, long_threshold=5)


def test_group_keys_contains_short():
    splitter = CSVLengthSplitter(_source(), column="name")
    assert "short" in splitter.group_keys


def test_group_keys_contains_medium():
    splitter = CSVLengthSplitter(_source(), column="name")
    assert "medium" in splitter.group_keys


def test_group_keys_contains_long():
    splitter = CSVLengthSplitter(_source(), column="name")
    assert "long" in splitter.group_keys


def test_short_row_count():
    splitter = CSVLengthSplitter(_source(), column="name")
    assert splitter.row_count_for("short") == 2


def test_medium_row_count():
    splitter = CSVLengthSplitter(_source(), column="name")
    assert splitter.row_count_for("medium") == 3


def test_long_row_count():
    splitter = CSVLengthSplitter(_source(), column="name")
    assert splitter.row_count_for("long") == 2


def test_short_rows_values():
    splitter = CSVLengthSplitter(_source(), column="name")
    names = [r["name"] for r in splitter.rows_for("short")]
    assert names == ["Al", "Bob"]


def test_long_rows_values():
    splitter = CSVLengthSplitter(_source(), column="name")
    names = [r["name"] for r in splitter.rows_for("long")]
    assert names == ["Christopher", "Bartholomew"]


def test_unknown_key_raises():
    splitter = CSVLengthSplitter(_source(), column="name")
    with pytest.raises(KeyError, match="Unknown length group"):
        list(splitter.rows_for("xlarge"))


def test_custom_thresholds():
    splitter = CSVLengthSplitter(
        _source(), column="name", short_threshold=3, long_threshold=7
    )
    # len<3: "Al"(2)  -> short
    # 3<=len<7: "Bob"(3),"Clara"(5),"Daniel"(6) -> medium
    # len>=7: "Elizabeth"(9),"Christopher"(11),"Bartholomew"(11) -> long
    assert splitter.row_count_for("short") == 1
    assert splitter.row_count_for("medium") == 3
    assert splitter.row_count_for("long") == 3


def test_empty_bucket_not_in_group_keys():
    src = _FakeSource(
        headers=["name"],
        rows=[{"name": "Christopher"}, {"name": "Bartholomew"}],
    )
    splitter = CSVLengthSplitter(src, column="name")
    # Both names are long; short and medium should be absent from group_keys
    assert "short" not in splitter.group_keys
    assert "medium" not in splitter.group_keys
    assert "long" in splitter.group_keys
