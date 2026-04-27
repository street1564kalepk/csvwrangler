"""Integration tests for Pipeline.split_by_type()."""

import io
import importlib

import pytest

from csvwrangler.pipeline import Pipeline
import csvwrangler.pipeline_splitter_by_type_init  # noqa: F401  – applies patch


CSV_TEXT = """id,value
1,42
2,3.14
3,hello
4,
5,true
6,99
7,world
"""


def pipeline():
    return Pipeline.from_string(CSV_TEXT)


def _parse(p):
    return [row for row in p._source.rows()]


def test_split_by_type_method_exists():
    assert hasattr(Pipeline, "split_by_type")


def test_split_by_type_returns_dict():
    result = pipeline().split_by_type("value")
    assert isinstance(result, dict)


def test_split_by_type_group_keys():
    result = pipeline().split_by_type("value")
    assert set(result.keys()) == {"int", "float", "string", "empty", "bool"}


def test_split_by_type_int_row_count():
    result = pipeline().split_by_type("value")
    rows = _parse(result["int"])
    assert len(rows) == 2


def test_split_by_type_float_values():
    result = pipeline().split_by_type("value")
    rows = _parse(result["float"])
    assert len(rows) == 1
    assert rows[0]["value"] == "3.14"


def test_split_by_type_string_values():
    result = pipeline().split_by_type("value")
    rows = _parse(result["string"])
    values = {r["value"] for r in rows}
    assert values == {"hello", "world"}


def test_split_by_type_headers_preserved():
    result = pipeline().split_by_type("value")
    for p in result.values():
        assert p._source.headers == ["id", "value"]


def test_split_by_type_total_rows_preserved():
    result = pipeline().split_by_type("value")
    total = sum(len(_parse(p)) for p in result.values())
    # CSV has 7 data rows
    assert total == 7
