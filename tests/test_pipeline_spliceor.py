"""Pipeline-level tests for .splice()."""
import io
import pytest
from csvwrangler.pipeline import Pipeline
import csvwrangler.pipeline_spliceor_patch  # noqa: F401  – registers .splice()


def _parse(text: str) -> Pipeline:
    return Pipeline.from_string(text.strip())


_LEFT_CSV = """
name,dept
Alice,Eng
Bob,HR
Carol,Eng
"""

_RIGHT_CSV = """
name,dept
Dave,Sales
"""


def pipeline():
    return _parse(_LEFT_CSV)


def test_splice_method_exists():
    assert hasattr(Pipeline, "splice")


def test_splice_append_row_count():
    left = _parse(_LEFT_CSV)
    right = _parse(_RIGHT_CSV)
    result = left.splice(right)
    rows = list(result._source.rows())
    assert len(rows) == 4


def test_splice_append_order():
    left = _parse(_LEFT_CSV)
    right = _parse(_RIGHT_CSV)
    rows = list(left.splice(right)._source.rows())
    assert rows[-1]["name"] == "Dave"


def test_splice_insert_mid():
    left = _parse(_LEFT_CSV)
    right = _parse(_RIGHT_CSV)
    rows = list(left.splice(right, after_row=0)._source.rows())
    assert rows[1]["name"] == "Dave"
    assert rows[0]["name"] == "Alice"


def test_splice_headers_unchanged():
    left = _parse(_LEFT_CSV)
    right = _parse(_RIGHT_CSV)
    assert left.splice(right)._source.headers() == ["name", "dept"]
