"""Tests for CSVValueSplitter."""
import pytest
from csvwrangler.splitter_by_value import CSVValueSplitter


class _FakeSource:
    def __init__(self, headers, data):
        self._headers = headers
        self._data = data

    @property
    def headers(self):
        return list(self._headers)

    def rows(self):
        yield from self._data


def _source():
    headers = ["name", "dept", "score"]
    data = [
        {"name": "Alice", "dept": "Eng", "score": "90"},
        {"name": "Bob",   "dept": "HR",  "score": "70"},
        {"name": "Carol", "dept": "Eng", "score": "85"},
        {"name": "Dave",  "dept": "HR",  "score": "60"},
        {"name": "Eve",   "dept": "Eng", "score": "95"},
    ]
    return _FakeSource(headers, data)


def test_headers_unchanged():
    vs = CSVValueSplitter(_source(), "dept")
    assert vs.headers == ["name", "dept", "score"]


def test_invalid_column_raises():
    with pytest.raises(ValueError, match="Column 'missing'"):
        CSVValueSplitter(_source(), "missing")


def test_group_keys_order():
    vs = CSVValueSplitter(_source(), "dept")
    assert vs.group_keys() == ["Eng", "HR"]


def test_rows_for_eng():
    vs = CSVValueSplitter(_source(), "dept")
    rows = vs.rows_for("Eng")
    assert len(rows) == 3
    assert all(r["dept"] == "Eng" for r in rows)


def test_rows_for_hr():
    vs = CSVValueSplitter(_source(), "dept")
    rows = vs.rows_for("HR")
    assert len(rows) == 2
    assert all(r["dept"] == "HR" for r in rows)


def test_rows_for_missing_key_returns_empty():
    vs = CSVValueSplitter(_source(), "dept")
    assert vs.rows_for("Finance") == []


def test_all_groups_keys():
    vs = CSVValueSplitter(_source(), "dept")
    groups = vs.all_groups()
    assert set(groups.keys()) == {"Eng", "HR"}


def test_all_groups_total_rows():
    vs = CSVValueSplitter(_source(), "dept")
    groups = vs.all_groups()
    total = sum(len(v) for v in groups.values())
    assert total == 5


def test_row_count():
    vs = CSVValueSplitter(_source(), "dept")
    assert vs.row_count() == 5


def test_names_in_eng_group():
    vs = CSVValueSplitter(_source(), "dept")
    names = [r["name"] for r in vs.rows_for("Eng")]
    assert names == ["Alice", "Carol", "Eve"]


def test_split_by_score_bucket():
    headers = ["name", "band"]
    data = [
        {"name": "A", "band": "high"},
        {"name": "B", "band": "low"},
        {"name": "C", "band": "high"},
    ]
    src = _FakeSource(headers, data)
    vs = CSVValueSplitter(src, "band")
    assert vs.group_keys() == ["high", "low"]
    assert len(vs.rows_for("high")) == 2
    assert len(vs.rows_for("low")) == 1
