"""Pipeline integration tests for mask_columns."""
import io
from csvwrangler.pipeline import Pipeline

_CSV = """name,email,score
Alice,alice@example.com,95
Bob,bob@example.com,80
"""


def pipeline():
    return Pipeline.from_string(_CSV)


def _parse(text: str) -> list[dict]:
    import csv
    reader = csv.DictReader(io.StringIO(text))
    return list(reader)


def test_mask_columns_method_exists():
    p = pipeline()
    assert hasattr(p, "mask_columns")


def test_mask_columns_headers_unchanged():
    p = pipeline()
    out = p.mask_columns(["email"]).to_string()
    rows = _parse(out)
    assert set(rows[0].keys()) == {"name", "email", "score"}


def test_mask_columns_redacts_email():
    p = pipeline()
    out = p.mask_columns(["email"]).to_string()
    rows = _parse(out)
    assert "@" not in rows[0]["email"]
    assert "*" in rows[0]["email"]


def test_mask_columns_partial_strategy():
    p = pipeline()
    out = p.mask_columns(["name"], strategy="partial", visible=2).to_string()
    rows = _parse(out)
    assert rows[0]["name"].startswith("Al")
    assert "*" in rows[0]["name"]


def test_mask_columns_non_masked_col_unchanged():
    p = pipeline()
    out = p.mask_columns(["email"]).to_string()
    rows = _parse(out)
    assert rows[0]["name"] == "Alice"
    assert rows[0]["score"] == "95"
