"""Pipeline integration tests for correlate()."""
import math
import io
import pytest
from csvwrangler.pipeline import Pipeline


_CSV = """x,y,z
1,2,9
2,4,8
3,6,7
4,8,6
5,10,5
"""


def pipeline():
    return Pipeline.from_string(_CSV)


def test_correlate_method_exists():
    p = pipeline()
    assert hasattr(p, "correlate")


def test_correlate_returns_matrix():
    p = pipeline()
    m = p.correlate(["x", "y"]).correlation_matrix
    assert "x" in m and "y" in m


def test_correlate_xy_perfect():
    p = pipeline()
    m = p.correlate(["x", "y"]).correlation_matrix
    assert math.isclose(m["x"]["y"], 1.0, abs_tol=1e-9)


def test_correlate_xz_anti():
    p = pipeline()
    m = p.correlate(["x", "z"]).correlation_matrix
    assert math.isclose(m["x"]["z"], -1.0, abs_tol=1e-9)


def test_correlate_does_not_drop_rows():
    p = pipeline()
    out = p.correlate(["x", "y"]).to_string()
    lines = [l for l in out.strip().splitlines() if l]
    assert len(lines) == 6  # header + 5 data rows
