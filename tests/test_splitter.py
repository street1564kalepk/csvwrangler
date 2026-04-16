"""Tests for CSVSplitter."""
import pytest
from csvwrangler.splitter import CSVSplitter


class _FakeSource:
    def __init__(self, headers, data):
        self._headers = headers
        self._data = data

    @property
    def headers(self):
        return self._headers

    def rows(self):
        yield from self._data


def _source():
    return _FakeSource(
        headers=["dept", "name", "salary"],
        data=[
            {"dept": "eng", "name": "Alice", "salary": "90000"},
            {"dept": "hr", "name": "Bob", "salary": "60000"},
            {"dept": "eng", "name": "Carol", "salary": "95000"},
            {"dept": "hr", "name": "Dave", "salary": "62000"},
            {"dept": "eng", "name": "Eve", "salary": "88000"},
        ],
    )


def test_headers_unchanged():
    sp = CSVSplitter(_source(), "dept")
    assert sp.headers == ["dept", "name", "salary"]


def test_invalid_column_raises():
    with pytest.raises(ValueError, match="not found in headers"):
        CSVSplitter(_source(), "nonexistent")


def test_invalid_source_raises():
    with pytest.raises(TypeError):
        CSVSplitter(object(), "dept")


def test_groups_keys():
    sp = CSVSplitter(_source(), "dept")
    groups = sp.groups()
    assert set(groups.keys()) == {"eng", "hr"}


def test_groups_eng_rows():
    sp = CSVSplitter(_source(), "dept")
    eng = sp.groups()["eng"]
    assert len(eng) == 3
    assert all(r["dept"] == "eng" for r in eng)


def test_groups_hr_rows():
    sp = CSVSplitter(_source(), "dept")
    hr = sp.groups()["hr"]
    assert len(hr) == 2
    assert all(r["dept"] == "hr" for r in hr)


def test_group_count():
    sp = CSVSplitter(_source(), "dept")
    assert sp.group_count() == 2


def test_row_count():
    sp = CSVSplitter(_source(), "dept")
    assert sp.row_count() == 5


def test_rows_yields_all():
    sp = CSVSplitter(_source(), "dept")
    result = list(sp.rows())
    assert len(result) == 5


def test_single_group():
    src = _FakeSource(
        headers=["cat", "val"],
        data=[{"cat": "x", "val": "1"}, {"cat": "x", "val": "2"}],
    )
    sp = CSVSplitter(src, "cat")
    assert sp.group_count() == 1
    assert len(sp.groups()["x"]) == 2
