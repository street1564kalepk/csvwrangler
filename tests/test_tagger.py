"""Tests for CSVTagger."""
import pytest
from csvwrangler.tagger import CSVTagger


class _FakeSource:
    def __init__(self, hdrs, data):
        self._headers = hdrs
        self._rows = data

    def headers(self):
        return list(self._headers)

    def rows(self):
        return iter(self._rows)

    def row_count(self):
        return len(self._rows)


def _source():
    return _FakeSource(
        ["name", "score"],
        [
            {"name": "Alice", "score": "92"},
            {"name": "Bob",   "score": "55"},
            {"name": "Carol", "score": "73"},
            {"name": "Dave",  "score": "40"},
        ],
    )


_RULES = [
    ("high",   lambda r: int(r["score"]) >= 80),
    ("medium", lambda r: int(r["score"]) >= 60),
    ("low",    lambda r: True),
]


def test_headers_include_new_column():
    t = CSVTagger(_source(), "grade", _RULES)
    assert t.headers() == ["name", "score", "grade"]


def test_row_count_unchanged():
    t = CSVTagger(_source(), "grade", _RULES)
    assert t.row_count() == 4


def test_first_rule_wins():
    rows = list(CSVTagger(_source(), "grade", _RULES).rows())
    assert rows[0]["grade"] == "high"
    assert rows[1]["grade"] == "low"
    assert rows[2]["grade"] == "medium"
    assert rows[3]["grade"] == "low"


def test_default_when_no_rule_matches():
    rules = [("high", lambda r: int(r["score"]) >= 95)]
    rows = list(CSVTagger(_source(), "grade", rules, default="other").rows())
    assert rows[0]["grade"] == "other"
    assert rows[1]["grade"] == "other"


def test_original_columns_preserved():
    rows = list(CSVTagger(_source(), "grade", _RULES).rows())
    assert rows[0]["name"] == "Alice"
    assert rows[0]["score"] == "92"


def test_empty_rules_raises():
    with pytest.raises(ValueError, match="at least one rule"):
        CSVTagger(_source(), "grade", [])


def test_empty_column_raises():
    with pytest.raises(ValueError, match="column name"):
        CSVTagger(_source(), "", _RULES)


def test_predicate_exception_falls_through():
    """A rule that raises should be skipped silently."""
    bad_rule = ("boom", lambda r: 1 / 0)
    rules = [bad_rule, ("safe", lambda r: True)]
    rows = list(CSVTagger(_source(), "grade", rules).rows())
    assert all(r["grade"] == "safe" for r in rows)
