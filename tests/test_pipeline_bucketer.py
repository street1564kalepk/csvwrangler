"""Integration tests for Pipeline.bucket_column()."""
import io
import pytest
from csvwrangler.pipeline import Pipeline
from csvwrangler.pipeline_bucketer_patch import _patch

_patch(Pipeline)

_CSV = """name,score
Alice,15
Bob,45
Carol,75
Dave,95
"""

_BUCKETS = [("low", 25.0), ("mid", 60.0), ("high", 85.0)]


def pipeline():
    return Pipeline.from_string(_CSV)


def _parse(p):
    out = io.StringIO()
    p.to_file(out)
    out.seek(0)
    import csv
    return list(csv.DictReader(out))


def test_bucket_column_method_exists():
    assert hasattr(Pipeline, "bucket_column")


def test_bucket_column_headers_appended():
    p = pipeline().bucket_column("score", _BUCKETS)
    rows = _parse(p)
    assert "bucket" in rows[0]


def test_bucket_column_custom_target():
    p = pipeline().bucket_column("score", _BUCKETS, target_column="tier")
    rows = _parse(p)
    assert "tier" in rows[0]
    assert "bucket" not in rows[0]


def test_bucket_column_low_value():
    p = pipeline().bucket_column("score", _BUCKETS)
    rows = _parse(p)
    alice = next(r for r in rows if r["name"] == "Alice")
    assert alice["bucket"] == "low"


def test_bucket_column_mid_value():
    p = pipeline().bucket_column("score", _BUCKETS)
    rows = _parse(p)
    bob = next(r for r in rows if r["name"] == "Bob")
    assert bob["bucket"] == "mid"


def test_bucket_column_default_for_overflow():
    p = pipeline().bucket_column("score", _BUCKETS)
    rows = _parse(p)
    dave = next(r for r in rows if r["name"] == "Dave")
    assert dave["bucket"] == "other"


def test_bucket_column_chainable_with_filter():
    p = (
        pipeline()
        .bucket_column("score", _BUCKETS)
        .where("bucket", "eq", "mid")
    )
    rows = _parse(p)
    assert all(r["bucket"] == "mid" for r in rows)
    assert len(rows) == 1
