"""Pipeline integration tests for chunking via Pipeline.chunk()."""
import io
import pytest
from csvwrangler.pipeline import Pipeline


_CSV = """id,city,score
1,London,90
2,Paris,80
3,Berlin,70
4,Rome,60
5,Madrid,50
"""


def pipeline():
    return Pipeline.from_string(_CSV)


def _parse(text: str):
    import csv
    reader = csv.DictReader(io.StringIO(text))
    return list(reader)


def test_chunk_method_exists():
    p = pipeline()
    assert hasattr(p, "chunk")


def test_chunk_returns_list_of_strings():
    p = pipeline()
    results = p.chunk(size=2)
    assert isinstance(results, list)
    assert len(results) == 3  # 5 rows -> chunks of 2,2,1


def test_chunk_first_chunk_has_two_rows():
    results = pipeline().chunk(size=2)
    rows = _parse(results[0])
    assert len(rows) == 2
    assert rows[0]["id"] == "1"
    assert rows[1]["id"] == "2"


def test_chunk_last_chunk_has_one_row():
    results = pipeline().chunk(size=2)
    rows = _parse(results[-1])
    assert len(rows) == 1
    assert rows[0]["id"] == "5"


def test_chunk_headers_present_in_every_chunk():
    results = pipeline().chunk(size=2)
    for csv_text in results:
        rows = _parse(csv_text)
        assert "id" in rows[0]
        assert "city" in rows[0]


def test_chunk_combined_rows_equal_source():
    results = pipeline().chunk(size=3)
    all_rows = []
    for csv_text in results:
        all_rows.extend(_parse(csv_text))
    assert len(all_rows) == 5
    assert [r["id"] for r in all_rows] == ["1", "2", "3", "4", "5"]
