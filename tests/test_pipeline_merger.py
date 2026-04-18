"""Pipeline integration tests for the merge step."""
import textwrap
from csvwrangler.pipeline import Pipeline


_LEFT_CSV = textwrap.dedent("""\
    id,name,city
    1,Alice,London
    2,Bob,Paris
    3,Carol,Berlin
""").strip()

_RIGHT_CSV = textwrap.dedent("""\
    id,score
    1,95
    2,80
""").strip()


def _left_pipeline():
    return Pipeline.from_string(_LEFT_CSV)


def _right_pipeline():
    return Pipeline.from_string(_RIGHT_CSV)


def _parse(p: Pipeline):
    import csv, io
    out = p.to_string()
    reader = csv.DictReader(io.StringIO(out))
    return list(reader)


def test_merge_method_exists():
    p = _left_pipeline()
    assert hasattr(p, "merge")


def test_merge_headers():
    p = _left_pipeline().merge(_right_pipeline(), on="id")
    rows = _parse(p)
    assert set(rows[0].keys()) == {"id", "name", "city", "score"}


def test_merge_matched_values():
    rows = _parse(_left_pipeline().merge(_right_pipeline(), on="id"))
    alice = next(r for r in rows if r["name"] == "Alice")
    assert alice["score"] == "95"


def test_merge_unmatched_empty():
    rows = _parse(_left_pipeline().merge(_right_pipeline(), on="id"))
    carol = next(r for r in rows if r["name"] == "Carol")
    assert carol["score"] == ""


def test_merge_row_count():
    rows = _parse(_left_pipeline().merge(_right_pipeline(), on="id"))
    assert len(rows) == 3
