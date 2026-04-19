"""Tests for CSVMasker."""
import pytest
from csvwrangler.masker import CSVMasker


class _FakeSource:
    def __init__(self, headers, data):
        self.headers = headers
        self._data = data

    def rows(self):
        return iter(self._data)


def _source():
    return _FakeSource(
        ["name", "email", "score"],
        [
            {"name": "Alice", "email": "alice@example.com", "score": "95"},
            {"name": "Bob", "email": "bob@example.com", "score": "80"},
            {"name": "Charlie", "email": "charlie@example.com", "score": "72"},
        ],
    )


def test_headers_unchanged():
    m = CSVMasker(_source(), ["email"])
    assert m.headers == ["name", "email", "score"]


def test_redact_full_mask():
    m = CSVMasker(_source(), ["email"], strategy="redact")
    rows = list(m.rows())
    assert rows[0]["email"] == "*" * len("alice@example.com")
    assert rows[0]["name"] == "Alice"


def test_partial_mask_keeps_leading_chars():
    m = CSVMasker(_source(), ["name"], strategy="partial", visible=2)
    rows = list(m.rows())
    assert rows[0]["name"].startswith("Al")
    assert "*" in rows[0]["name"]
    assert len(rows[0]["name"]) == len("Alice")


def test_fixed_mask_always_six_chars():
    m = CSVMasker(_source(), ["email"], strategy="fixed", char="#")
    rows = list(m.rows())
    for row in rows:
        assert row["email"] == "######"


def test_multiple_columns_masked():
    m = CSVMasker(_source(), ["name", "email"])
    rows = list(m.rows())
    assert rows[1]["name"] == "***"
    assert rows[1]["score"] == "80"


def test_unknown_column_raises():
    with pytest.raises(ValueError, match="Unknown columns"):
        CSVMasker(_source(), ["nonexistent"])


def test_unknown_strategy_raises():
    with pytest.raises(ValueError, match="Unknown strategy"):
        CSVMasker(_source(), ["email"], strategy="invisible")


def test_row_count():
    m = CSVMasker(_source(), ["email"])
    assert m.row_count == 3


def test_custom_char():
    m = CSVMasker(_source(), ["score"], strategy="redact", char="X")
    rows = list(m.rows())
    assert rows[0]["score"] == "XX"
