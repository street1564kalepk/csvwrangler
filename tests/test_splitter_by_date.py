"""Tests for CSVDateSplitter."""
import pytest
from csvwrangler.splitter_by_date import CSVDateSplitter


class _FakeSource:
    def __init__(self, headers, rows):
        self._headers = headers
        self._rows = rows

    def headers(self):
        return list(self._headers)

    def rows(self):
        yield from self._rows


def _source():
    hdrs = ["id", "date", "value"]
    data = [
        {"id": "1", "date": "2024-01-05", "value": "10"},
        {"id": "2", "date": "2024-01-20", "value": "20"},
        {"id": "3", "date": "2024-02-03", "value": "30"},
        {"id": "4", "date": "2024-02-14", "value": "40"},
        {"id": "5", "date": "2024-03-01", "value": "50"},
        {"id": "6", "date": "2025-01-10", "value": "60"},
    ]
    return _FakeSource(hdrs, data)


def test_headers_unchanged():
    ds = CSVDateSplitter(_source(), "date", "month")
    assert ds.headers() == ["id", "date", "value"]


def test_invalid_period_raises():
    with pytest.raises(ValueError, match="period must be one of"):
        CSVDateSplitter(_source(), "date", "quarter")


def test_missing_column_raises():
    ds = CSVDateSplitter(_source(), "created_at", "month")
    with pytest.raises(KeyError):
        ds.group_keys()


def test_group_by_month_keys():
    ds = CSVDateSplitter(_source(), "date", "month")
    assert ds.group_keys() == ["2024-01", "2024-02", "2024-03", "2025-01"]


def test_group_by_month_count():
    ds = CSVDateSplitter(_source(), "date", "month")
    assert ds.group_count() == 4


def test_rows_for_january_2024():
    ds = CSVDateSplitter(_source(), "date", "month")
    rows = list(ds.rows_for("2024-01"))
    assert len(rows) == 2
    assert rows[0]["id"] == "1"
    assert rows[1]["id"] == "2"


def test_row_count_for_february():
    ds = CSVDateSplitter(_source(), "date", "month")
    assert ds.row_count_for("2024-02") == 2


def test_row_count_for_missing_key_is_zero():
    ds = CSVDateSplitter(_source(), "date", "month")
    assert ds.row_count_for("2099-12") == 0


def test_group_by_year():
    ds = CSVDateSplitter(_source(), "date", "year")
    assert ds.group_keys() == ["2024", "2025"]
    assert ds.row_count_for("2024") == 5
    assert ds.row_count_for("2025") == 1


def test_group_by_day():
    ds = CSVDateSplitter(_source(), "date", "day")
    assert "2024-01-05" in ds.group_keys()
    assert ds.row_count_for("2024-01-05") == 1


def test_group_by_week():
    ds = CSVDateSplitter(_source(), "date", "week")
    keys = ds.group_keys()
    # 2024-01-05 is ISO week 1 of 2024
    assert "2024-W01" in keys


def test_malformed_date_goes_to_unknown():
    hdrs = ["id", "date"]
    data = [{"id": "1", "date": "not-a-date"}, {"id": "2", "date": "2024-06-01"}]
    ds = CSVDateSplitter(_FakeSource(hdrs, data), "date", "month")
    assert "unknown" in ds.group_keys()
    assert ds.row_count_for("unknown") == 1
