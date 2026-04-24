"""Integration tests for Pipeline.clip_by_rank()."""
import io
import pytest
from csvwrangler.pipeline import Pipeline
from csvwrangler.pipeline_clipper_by_rank_patch import _patch

_patch(Pipeline)

_CSV = """name,score
Alice,85
Bob,92
Carol,78
Dave,95
Eve,60
"""


def pipeline():
    return Pipeline.from_string(_CSV)


def _parse(csv_text: str) -> list[dict]:
    rows = []
    for line in csv_text.strip().splitlines()[1:]:
        name, score = line.split(",")
        rows.append({"name": name, "score": score})
    return rows


def test_clip_by_rank_method_exists():
    assert hasattr(Pipeline, "clip_by_rank")


def test_clip_by_rank_top_headers_unchanged():
    result = pipeline().clip_by_rank("score", 3).to_string()
    header_line = result.strip().splitlines()[0]
    assert header_line == "name,score"


def test_clip_by_rank_top_row_count():
    result = pipeline().clip_by_rank("score", 3, direction="top").to_string()
    rows = result.strip().splitlines()[1:]
    assert len(rows) == 3


def test_clip_by_rank_top_contains_highest():
    result = pipeline().clip_by_rank("score", 2, direction="top").to_string()
    assert "Dave" in result
    assert "Bob" in result


def test_clip_by_rank_bottom_contains_lowest():
    result = pipeline().clip_by_rank("score", 2, direction="bottom").to_string()
    assert "Eve" in result
    assert "Carol" in result


def test_clip_by_rank_n_zero_returns_header_only():
    result = pipeline().clip_by_rank("score", 0).to_string()
    lines = result.strip().splitlines()
    assert len(lines) == 1
    assert lines[0] == "name,score"


def test_clip_by_rank_invalid_direction_raises():
    with pytest.raises(ValueError):
        pipeline().clip_by_rank("score", 2, direction="sideways")
