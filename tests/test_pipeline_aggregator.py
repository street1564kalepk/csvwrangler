import csv
import io
import pytest
from csvwrangler.pipeline import Pipeline
from csvwrangler.reader import CSVReader


CSV_DATA = """region,product,amount,qty
North,A,100,2
North,A,200,3
South,B,150,1
South,B,50,4
North,B,80,1
"""


@pytest.fixture
def pipeline(tmp_path):
    f = tmp_path / "sales.csv"
    f.write_text(CSV_DATA)
    reader = CSVReader(str(f))
    return Pipeline(reader)


def _parse(csv_string):
    reader = csv.DictReader(io.StringIO(csv_string))
    return list(reader)


def test_aggregate_headers(pipeline):
    p = pipeline.aggregate(["region"], {"total": "sum:amount"})
    assert p.headers == ["region", "total"]


def test_aggregate_sum_to_string(pipeline):
    result = pipeline.aggregate(["region"], {"total": "sum:amount"}).to_string()
    rows = {r["region"]: float(r["total"]) for r in _parse(result)}
    assert rows["North"] == pytest.approx(380.0)
    assert rows["South"] == pytest.approx(200.0)


def test_aggregate_count_to_string(pipeline):
    result = pipeline.aggregate(["region"], {"cnt": "count:amount"}).to_string()
    rows = {r["region"]: int(r["cnt"]) for r in _parse(result)}
    assert rows["North"] == 3
    assert rows["South"] == 2


def test_aggregate_to_file(pipeline, tmp_path):
    out = tmp_path / "agg.csv"
    pipeline.aggregate(["region"], {"total": "sum:amount"}).to_file(str(out))
    assert out.exists()
    rows = _parse(out.read_text())
    assert len(rows) == 2
    assert all("region" in r and "total" in r for r in rows)


def test_filter_then_aggregate(pipeline):
    result = (
        pipeline
        .where("region", "eq", "North")
        .aggregate(["product"], {"s": "sum:amount"})
        .to_string()
    )
    rows = {r["product"]: float(r["s"]) for r in _parse(result)}
    assert rows["A"] == pytest.approx(300.0)
    assert rows["B"] == pytest.approx(80.0)


def test_aggregate_then_sort(pipeline):
    result = (
        pipeline
        .aggregate(["region"], {"total": "sum:amount"})
        .sort("total", descending=True)
        .to_string()
    )
    rows = _parse(result)
    totals = [float(r["total"]) for r in rows]
    assert totals == sorted(totals, reverse=True)


def test_aggregate_mean(pipeline):
    result = pipeline.aggregate(["region"], {"avg": "mean:amount"}).to_string()
    rows = {r["region"]: float(r["avg"]) for r in _parse(result)}
    assert rows["North"] == pytest.approx(380.0 / 3)
    assert rows["South"] == pytest.approx(100.0)


def test_aggregate_multiple_keys(pipeline):
    """Grouping by multiple columns should produce one row per unique combination."""
    result = pipeline.aggregate(["region", "product"], {"total": "sum:amount"}).to_string()
    rows = _parse(result)
    # CSV_DATA has 3 unique (region, product) pairs: North/A, North/B, South/B
    assert len(rows) == 3
    key_map = {(r["region"], r["product"]): float(r["total"]) for r in rows}
    assert key_map[("North", "A")] == pytest.approx(300.0)
    assert key_map[("North", "B")] == pytest.approx(80.0)
    assert key_map[("South", "B")] == pytest.approx(200.0)
