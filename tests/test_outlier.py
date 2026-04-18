import pytest
from csvwrangler.outlier import CSVOutlier


class _FakeSource:
    def __init__(self, hdrs, data):
        self._headers = hdrs
        self._data = data

    @property
    def headers(self):
        return list(self._headers)

    def rows(self):
        for row in self._data:
            yield dict(row)


def _source():
    # values: 1,2,3,4,5,6,7,8,9,100  – 100 is a clear outlier
    data = [{"name": f"r{i}", "score": str(v)}
            for i, v in enumerate([1, 2, 3, 4, 5, 6, 7, 8, 9, 100])]
    return _FakeSource(["name", "score"], data)


def test_flag_mode_headers():
    o = CSVOutlier(_source(), "score", mode="flag")
    assert o.headers == ["name", "score", "is_outlier"]


def test_drop_mode_headers():
    o = CSVOutlier(_source(), "score", mode="drop")
    assert o.headers == ["name", "score"]


def test_flag_outlier_marked():
    o = CSVOutlier(_source(), "score", mode="flag")
    flagged = [r for r in o.rows() if r["is_outlier"] == "1"]
    assert len(flagged) == 1
    assert flagged[0]["score"] == "100"


def test_flag_non_outliers_zero():
    o = CSVOutlier(_source(), "score", mode="flag")
    normal = [r for r in o.rows() if r["is_outlier"] == "0"]
    assert len(normal) == 9


def test_drop_removes_outlier():
    o = CSVOutlier(_source(), "score", mode="drop")
    result = list(o.rows())
    assert len(result) == 9
    assert all(r["score"] != "100" for r in result)


def test_row_count_drop():
    o = CSVOutlier(_source(), "score", mode="drop")
    assert o.row_count() == 9


def test_custom_flag_column():
    o = CSVOutlier(_source(), "score", mode="flag", flag_column="outlier_flag")
    assert "outlier_flag" in o.headers
    rows = list(o.rows())
    assert "outlier_flag" in rows[0]


def test_invalid_mode_raises():
    with pytest.raises(ValueError, match="mode"):
        CSVOutlier(_source(), "score", mode="invalid")


def test_missing_column_raises():
    with pytest.raises(ValueError, match="not found"):
        CSVOutlier(_source(), "nonexistent")


def test_custom_k_tighter():
    # k=0 means anything outside Q1..Q3 is an outlier
    o = CSVOutlier(_source(), "score", k=0, mode="flag")
    flagged = [r for r in o.rows() if r["is_outlier"] == "1"]
    assert len(flagged) >= 1
