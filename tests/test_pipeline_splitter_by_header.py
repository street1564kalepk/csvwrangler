"""Pipeline-level tests for split_by_header."""

from __future__ import annotations

import io
import pytest

from csvwrangler.pipeline import Pipeline
from csvwrangler.pipeline_splitter_by_header_patch import _patch

_patch(Pipeline)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


_CSV = (
    "id,sales_q1,sales_q2,cost_q1,cost_q2\n"
    "1,100,200,40,50\n"
    "2,300,400,60,70\n"
)


def pipeline():
    return Pipeline.from_string(_CSV)


def _groups():
    return pipeline().split_by_header()


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_split_by_header_method_exists():
    assert hasattr(Pipeline, "split_by_header")


def test_split_returns_dict():
    groups = _groups()
    assert isinstance(groups, dict)


def test_split_group_keys():
    groups = _groups()
    assert set(groups.keys()) == {"", "sales", "cost"}


def test_split_sales_headers():
    groups = _groups()
    assert groups["sales"].headers == ["sales_q1", "sales_q2"]


def test_split_cost_headers():
    groups = _groups()
    assert groups["cost"].headers == ["cost_q1", "cost_q2"]


def test_split_sales_to_string_contains_values():
    groups = _groups()
    out = groups["sales"].to_string()
    assert "100" in out
    assert "400" in out


def test_split_no_prefix_group_has_id():
    groups = _groups()
    assert groups[""].headers == ["id"]
    rows = list(groups[""]._source.rows())
    assert rows[0]["id"] == "1"


def test_custom_separator():
    csv_text = "a.x,a.y,b.z\n1,2,3\n4,5,6\n"
    p = Pipeline.from_string(csv_text)
    groups = p.split_by_header(separator=".")
    assert set(groups.keys()) == {"a", "b"}
    assert groups["a"].headers == ["a.x", "a.y"]
