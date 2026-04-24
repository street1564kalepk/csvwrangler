"""Tests for CSVHighlighter."""
import pytest
from csvwrangler.highlighter import CSVHighlighter


class _FakeSource:
    def __init__(self, headers, data):
        self._headers = headers
        self._data = data

    @property
    def headers(self):
        return list(self._headers)

    def rows(self):
        for row in self._data:
            yield dict(row)


def _source():
    return _FakeSource(
        ["name", "dept", "score"],
        [
            {"name": "Alice", "dept": "Eng", "score": "95"},
            {"name": "Bob",   "dept": "HR",  "score": "60"},
            {"name": "Carol", "dept": "Eng", "score": "82"},
            {"name": "Dave",  "dept": "HR",  "score": "45"},
        ],
    )


def test_headers_appended():
    h = CSVHighlighter(_source(), lambda r: True)
    assert h.headers == ["name", "dept", "score", "highlighted"]


def test_custom_flag_column_name():
    h = CSVHighlighter(_source(), lambda r: True, flag_column="is_top")
    assert "is_top" in h.headers


def test_true_rows_flagged():
    h = CSVHighlighter(_source(), lambda r: r["dept"] == "Eng")
    flagged = [r["highlighted"] for r in h.rows()]
    assert flagged == ["true", "false", "true", "false"]


def test_custom_true_false_values():
    h = CSVHighlighter(
        _source(),
        lambda r: int(r["score"]) >= 80,
        true_value="yes",
        false_value="no",
    )
    result = {r["name"]: r["highlighted"] for r in h.rows()}
    assert result["Alice"] == "yes"
    assert result["Bob"] == "no"
    assert result["Carol"] == "yes"
    assert result["Dave"] == "no"


def test_row_count_unchanged():
    h = CSVHighlighter(_source(), lambda r: False)
    assert h.row_count == 4


def test_original_data_preserved():
    h = CSVHighlighter(_source(), lambda r: r["dept"] == "Eng")
    rows = list(h.rows())
    assert rows[0]["name"] == "Alice"
    assert rows[0]["score"] == "95"


def test_duplicate_flag_column_raises():
    src = _FakeSource(["name", "highlighted"], [{"name": "X", "highlighted": "y"}])
    with pytest.raises(ValueError, match="already exists"):
        CSVHighlighter(src, lambda r: True)


def test_all_false_when_predicate_never_matches():
    h = CSVHighlighter(_source(), lambda r: False)
    assert all(r["highlighted"] == "false" for r in h.rows())
