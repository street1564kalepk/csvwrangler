import pytest
from csvwrangler.summarizer import CSVSummarizer


class _FakeSource:
    def __init__(self, headers, data):
        self._headers = headers
        self._data = data

    @property
    def headers(self):
        return self._headers

    def rows(self):
        for row in self._data:
            yield dict(zip(self._headers, row))


def _source():
    return _FakeSource(
        ["name", "score", "age"],
        [
            ["Alice", "90", "30"],
            ["Bob", "80", "25"],
            ["Alice", "70", "30"],
            ["Charlie", "85", "35"],
        ],
    )


def test_headers():
    s = CSVSummarizer(_source())
    assert s.headers == ["column", "count", "unique", "min", "max", "mean"]


def test_row_count_equals_column_count():
    s = CSVSummarizer(_source())
    assert s.row_count == 3


def test_rows_returns_one_per_column():
    s = CSVSummarizer(_source())
    result = list(s.rows())
    assert len(result) == 3


def test_summary_column_names():
    s = CSVSummarizer(_source())
    cols = [r["column"] for r in s.rows()]
    assert cols == ["name", "score", "age"]


def test_count_is_total_rows():
    s = CSVSummarizer(_source())
    rows = {r["column"]: r for r in s.rows()}
    assert rows["score"]["count"] == 4


def test_unique_count():
    s = CSVSummarizer(_source())
    rows = {r["column"]: r for r in s.rows()}
    assert rows["name"]["unique"] == 3  # Alice, Bob, Charlie


def test_numeric_min_max_mean():
    s = CSVSummarizer(_source())
    rows = {r["column"]: r for r in s.rows()}
    assert rows["score"]["min"] == 70.0
    assert rows["score"]["max"] == 90.0
    assert rows["score"]["mean"] == pytest.approx(81.25)


def test_non_numeric_column_empty_stats():
    s = CSVSummarizer(_source())
    rows = {r["column"]: r for r in s.rows()}
    assert rows["name"]["min"] == ""
    assert rows["name"]["max"] == ""
    assert rows["name"]["mean"] == ""


def test_empty_source():
    src = _FakeSource(["x", "y"], [])
    s = CSVSummarizer(src)
    result = list(s.rows())
    assert len(result) == 2
    for r in result:
        assert r["count"] == 0
        assert r["mean"] == ""
