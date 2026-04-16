"""Integration tests: Pipeline .fill() step."""

import io
import csv
import pytest
from csvwrangler.pipeline import Pipeline


_CSV_TEXT = """name,age,city
Alice,30,London
,25,
Bob,,Paris
"""


def _pipeline():
    return Pipeline.from_source(io.StringIO(_CSV_TEXT))


def _parse(csv_string: str):
    return list(csv.DictReader(io.StringIO(csv_string)))


def test_fill_headers_unchanged():
    result = _pipeline().fill({"name": "Unknown", "city": "N/A"}).to_string()
    rows = _parse(result)
    assert list(rows[0].keys()) == ["name", "age", "city"]


def test_fill_replaces_empty_name():
    result = _pipeline().fill({"name": "Unknown"}).to_string()
    rows = _parse(result)
    assert rows[1]["name"] == "Unknown"


def test_fill_replaces_empty_city():
    result = _pipeline().fill({"city": "N/A"}).to_string()
    rows = _parse(result)
    assert rows[1]["city"] == "N/A"
    assert rows[0]["city"] == "London"  # untouched


def test_fill_multiple_columns():
    result = _pipeline().fill({"name": "Anon", "age": "0", "city": "Unknown"}).to_string()
    rows = _parse(result)
    assert rows[1] == {"name": "Anon", "age": "25", "city": "Unknown"}
    assert rows[2] == {"name": "Bob", "age": "0", "city": "Paris"}


def test_fill_row_count_unchanged():
    result = _pipeline().fill({"name": "X"}).to_string()
    rows = _parse(result)
    assert len(rows) == 3


def test_fill_chained_with_filter():
    result = (
        _pipeline()
        .fill({"name": "Unknown"})
        .where("name", "eq", "Unknown")
        .to_string()
    )
    rows = _parse(result)
    assert len(rows) == 1
    assert rows[0]["age"] == "25"
