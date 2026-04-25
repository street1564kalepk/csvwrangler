"""Pipeline-level tests for zip_by_position."""
import io
import csv
import pytest

import csvwrangler.pipeline_zipper_by_position_patch  # noqa: F401 – apply patch
from csvwrangler.pipeline import Pipeline


_LEFT_CSV = "name,score\nAlice,90\nBob,80\nCarol,70\n"
_RIGHT_CSV = "dept,level\nEng,senior\nHR,junior\n"


def _pipeline(text: str) -> Pipeline:
    return Pipeline.from_string(text)


def _parse(pipeline: Pipeline) -> list:
    buf = io.StringIO()
    pipeline.to_stream(buf)
    buf.seek(0)
    return list(csv.DictReader(buf))


# ---- method existence ----

def test_zip_by_position_method_exists():
    p = _pipeline(_LEFT_CSV)
    assert hasattr(p, "zip_by_position")


# ---- append mode (default) ----

def test_zip_by_position_append_headers():
    left = _pipeline(_LEFT_CSV)
    right = _pipeline(_RIGHT_CSV)
    result = left.zip_by_position(right)
    assert result._source.headers == ["name", "score", "dept", "level"]


def test_zip_by_position_append_row_count():
    left = _pipeline(_LEFT_CSV)
    right = _pipeline(_RIGHT_CSV)
    rows = _parse(left.zip_by_position(right))
    assert len(rows) == 3  # max(3, 2)


def test_zip_by_position_append_first_row_values():
    left = _pipeline(_LEFT_CSV)
    right = _pipeline(_RIGHT_CSV)
    rows = _parse(left.zip_by_position(right))
    assert rows[0]["name"] == "Alice"
    assert rows[0]["dept"] == "Eng"


def test_zip_by_position_append_padding():
    left = _pipeline(_LEFT_CSV)
    right = _pipeline(_RIGHT_CSV)
    rows = _parse(left.zip_by_position(right))
    assert rows[2]["dept"] == ""


# ---- interleave mode ----

def test_zip_by_position_interleave_headers():
    left = _pipeline(_LEFT_CSV)
    right = _pipeline(_RIGHT_CSV)
    result = left.zip_by_position(right, mode="interleave")
    assert result._source.headers == ["name", "dept", "score", "level"]


def test_zip_by_position_interleave_values():
    left = _pipeline(_LEFT_CSV)
    right = _pipeline(_RIGHT_CSV)
    rows = _parse(left.zip_by_position(right, mode="interleave"))
    assert rows[0]["name"] == "Alice"
    assert rows[0]["dept"] == "Eng"
    assert rows[0]["score"] == "90"
    assert rows[0]["level"] == "senior"


# ---- invalid mode ----

def test_zip_by_position_invalid_mode_raises():
    left = _pipeline(_LEFT_CSV)
    right = _pipeline(_RIGHT_CSV)
    with pytest.raises(ValueError):
        left.zip_by_position(right, mode="wrong")
