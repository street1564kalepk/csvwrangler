"""Integration tests for Pipeline.split_by_field_count."""

import io
import pytest
from csvwrangler.pipeline import Pipeline
from csvwrangler.pipeline_splitter_by_field_count_patch import _patch

_patch(Pipeline)


CSV_TEXT = """name,city,score
Alice,London,90
Bob,,80
Carol,Paris,
,, 
Dave,Rome,60
"""


def pipeline():
    return Pipeline.from_string(CSV_TEXT)


def _parse(p):
    return list(p._source.rows())


def test_split_by_field_count_method_exists():
    assert hasattr(Pipeline, "split_by_field_count")


def test_split_by_field_count_returns_dict():
    groups = pipeline().split_by_field_count()
    assert isinstance(groups, dict)


def test_split_by_field_count_group_keys_are_ints():
    groups = pipeline().split_by_field_count()
    for k in groups:
        assert isinstance(k, int)


def test_split_by_field_count_full_rows_in_group_3():
    groups = pipeline().split_by_field_count()
    rows = _parse(groups[3])
    names = [r["name"] for r in rows]
    assert "Alice" in names
    assert "Dave" in names


def test_split_by_field_count_partial_rows_in_group_2():
    groups = pipeline().split_by_field_count()
    rows = _parse(groups[2])
    assert len(rows) == 2


def test_split_by_field_count_headers_preserved():
    groups = pipeline().split_by_field_count()
    for p in groups.values():
        assert p._source.headers == ["name", "city", "score"]


def test_split_by_field_count_total_rows_preserved():
    groups = pipeline().split_by_field_count()
    total = sum(len(_parse(p)) for p in groups.values())
    assert total == 5
