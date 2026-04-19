"""Pipeline-level tests for .sequence()."""

import io
import pytest
from csvwrangler.pipeline import Pipeline
from csvwrangler.pipeline_sequencer_patch import _patch

_patch()

_CSV = "name,dept\nAlice,Eng\nBob,HR\nCarol,Eng\n"


def pipeline():
    return Pipeline.from_string(_CSV)


def _parse(csv_str: str) -> list[dict]:
    import csv
    return list(csv.DictReader(io.StringIO(csv_str)))


def test_sequence_method_exists():
    assert hasattr(pipeline(), "sequence")


def test_sequence_headers_first():
    p = pipeline().sequence(column="seq")
    assert p._source.headers[0] == "seq"


def test_sequence_headers_last():
    p = pipeline().sequence(column="n", position="last")
    assert p._source.headers[-1] == "n"


def test_sequence_values_in_output():
    buf = io.StringIO()
    pipeline().sequence().to_file(buf)
    rows = _parse(buf.getvalue())
    assert [r["seq"] for r in rows] == ["1", "2", "3"]


def test_sequence_custom_step():
    buf = io.StringIO()
    pipeline().sequence(start=0, step=5).to_file(buf)
    rows = _parse(buf.getvalue())
    assert rows[2]["seq"] == "10"


def test_sequence_chained_with_filter():
    buf = io.StringIO()
    pipeline().where("dept", "eq", "Eng").sequence().to_file(buf)
    rows = _parse(buf.getvalue())
    assert len(rows) == 2
    assert rows[0]["seq"] == "1"
