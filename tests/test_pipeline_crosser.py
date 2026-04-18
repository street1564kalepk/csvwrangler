"""Pipeline integration tests for the cross-product feature."""
import io
from csvwrangler.pipeline import Pipeline

_LEFT_CSV = "id,name\n1,Alice\n2,Bob\n"
_RIGHT_CSV = "color\nred\nblue\n"


def _left_pipeline():
    return Pipeline.from_string(_LEFT_CSV)


def _right_pipeline():
    return Pipeline.from_string(_RIGHT_CSV)


def _parse(pipeline):
    buf = io.StringIO()
    pipeline.to_file(buf)
    buf.seek(0)
    lines = [l.strip() for l in buf.readlines() if l.strip()]
    header = lines[0].split(",")
    rows = [dict(zip(header, l.split(","))) for l in lines[1:]]
    return header, rows


def test_cross_method_exists():
    p = _left_pipeline()
    assert hasattr(p, "cross")


def test_cross_headers():
    p = _left_pipeline().cross(_right_pipeline())
    header, _ = _parse(p)
    assert "left_id" in header
    assert "left_name" in header
    assert "right_color" in header


def test_cross_row_count():
    p = _left_pipeline().cross(_right_pipeline())
    _, rows = _parse(p)
    assert len(rows) == 4


def test_cross_values():
    p = _left_pipeline().cross(_right_pipeline())
    _, rows = _parse(p)
    alice_rows = [r for r in rows if r["left_name"] == "Alice"]
    assert len(alice_rows) == 2
    assert {r["right_color"] for r in alice_rows} == {"red", "blue"}
