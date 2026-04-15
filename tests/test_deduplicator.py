"""Tests for CSVDeduplicator."""
import io
import pytest
from csvwrangler.reader import CSVReader
from csvwrangler.deduplicator import CSVDeduplicator


CSV_DATA = (
    "name,city,score\n"
    "Alice,London,90\n"
    "Bob,Paris,85\n"
    "Alice,London,90\n"   # exact duplicate of row 1
    "Alice,Berlin,70\n"   # same name, different city
    "Bob,Paris,85\n"      # exact duplicate of row 2
)


def make_dedup(csv_text: str = CSV_DATA, key_columns=None) -> CSVDeduplicator:
    reader = CSVReader(io.StringIO(csv_text))
    return CSVDeduplicator(reader, key_columns=key_columns)


# ---------------------------------------------------------------------------
# Headers
# ---------------------------------------------------------------------------

def test_headers_unchanged():
    dedup = make_dedup()
    assert dedup.headers == ["name", "city", "score"]


# ---------------------------------------------------------------------------
# Full-row deduplication (no key_columns)
# ---------------------------------------------------------------------------

def test_dedup_all_columns_removes_exact_duplicates():
    dedup = make_dedup()
    result = list(dedup.rows())
    assert len(result) == 3


def test_dedup_all_columns_preserves_order():
    dedup = make_dedup()
    names = [r["name"] for r in dedup.rows()]
    assert names == ["Alice", "Bob", "Alice"]


def test_dedup_row_count():
    dedup = make_dedup()
    assert dedup.row_count() == 3


# ---------------------------------------------------------------------------
# Key-column deduplication
# ---------------------------------------------------------------------------

def test_dedup_by_name_only():
    dedup = make_dedup(key_columns=["name"])
    result = list(dedup.rows())
    # Only the first occurrence of each name is kept
    assert len(result) == 2
    assert result[0]["name"] == "Alice"
    assert result[1]["name"] == "Bob"


def test_dedup_by_name_and_city():
    dedup = make_dedup(key_columns=["name", "city"])
    result = list(dedup.rows())
    assert len(result) == 3
    cities = [r["city"] for r in result]
    assert cities == ["London", "Paris", "Berlin"]


# ---------------------------------------------------------------------------
# No duplicates present
# ---------------------------------------------------------------------------

def test_no_duplicates_returns_all_rows():
    csv_no_dups = "a,b\n1,x\n2,y\n3,z\n"
    dedup = make_dedup(csv_no_dups)
    assert dedup.row_count() == 3


# ---------------------------------------------------------------------------
# Chaining
# ---------------------------------------------------------------------------

def test_chain_with_filter():
    dedup = make_dedup()
    filtered = dedup.where("city", "eq", "London")
    result = list(filtered.rows())
    assert len(result) == 1
    assert result[0]["name"] == "Alice"
