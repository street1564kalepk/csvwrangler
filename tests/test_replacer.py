"""Tests for CSVReplacer."""
import pytest
from csvwrangler.replacer import CSVReplacer


class _FakeSource:
    def __init__(self, headers, data):
        self._headers = headers
        self._data = data

    @property
    def headers(self):
        return self._headers

    def rows(self):
        yield from (dict(r) for r in self._data)


def _source():
    return _FakeSource(
        ["name", "dept", "status"],
        [
            {"name": "Alice", "dept": "Eng", "status": "active"},
            {"name": "Bob",   "dept": "HR",  "status": "inactive"},
            {"name": "Carol", "dept": "Eng", "status": "active"},
        ],
    )


def test_headers_unchanged():
    r = CSVReplacer(_source(), {"dept": {"Eng": "Engineering"}})
    assert r.headers == ["name", "dept", "status"]


def test_exact_replacement():
    r = CSVReplacer(_source(), {"dept": {"Eng": "Engineering", "HR": "Human Resources"}})
    depts = [row["dept"] for row in r.rows()]
    assert depts == ["Engineering", "Human Resources", "Engineering"]


def test_exact_no_match_unchanged():
    r = CSVReplacer(_source(), {"dept": {"Finance": "Fin"}})
    depts = [row["dept"] for row in r.rows()]
    assert depts == ["Eng", "HR", "Eng"]


def test_multiple_columns():
    r = CSVReplacer(
        _source(),
        {"dept": {"Eng": "Engineering"}, "status": {"active": "1", "inactive": "0"}},
    )
    rows = list(r.rows())
    assert rows[0]["dept"] == "Engineering"
    assert rows[0]["status"] == "1"
    assert rows[1]["status"] == "0"


def test_substring_replacement():
    src = _FakeSource(
        ["label"],
        [{"label": "foo_bar"}, {"label": "foo_baz"}, {"label": "qux"}],
    )
    r = CSVReplacer(src, {"label": {"foo_": ""}}, substring=True)
    labels = [row["label"] for row in r.rows()]
    assert labels == ["bar", "baz", "qux"]


def test_unknown_column_raises():
    with pytest.raises(ValueError, match="Unknown columns"):
        CSVReplacer(_source(), {"nonexistent": {"a": "b"}})


def test_row_count():
    r = CSVReplacer(_source(), {"dept": {"Eng": "Engineering"}})
    assert r.row_count == 3
