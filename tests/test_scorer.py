import pytest
from csvwrangler.scorer import CSVScorer


class _FakeSource:
    def __init__(self, headers, data):
        self._headers = headers
        self._data = data

    @property
    def headers(self):
        return list(self._headers)

    def rows(self):
        return iter(self._data)


def _source():
    return _FakeSource(
        ["name", "math", "english"],
        [
            {"name": "Alice", "math": "90", "english": "80"},
            {"name": "Bob",   "math": "70", "english": "60"},
            {"name": "Carol", "math": "50", "english": "100"},
        ],
    )


def test_scorer_headers_include_score():
    s = CSVScorer(_source(), {"math": 0.6, "english": 0.4})
    assert s.headers == ["name", "math", "english", "score"]


def test_scorer_custom_score_col():
    s = CSVScorer(_source(), {"math": 1.0}, score_col="total")
    assert "total" in s.headers


def test_scorer_values_alice():
    s = CSVScorer(_source(), {"math": 0.6, "english": 0.4})
    rows = list(s.rows())
    # Alice: 90*0.6 + 80*0.4 = 54 + 32 = 86
    assert rows[0]["score"] == pytest.approx(86.0)


def test_scorer_values_bob():
    s = CSVScorer(_source(), {"math": 0.6, "english": 0.4})
    rows = list(s.rows())
    # Bob: 70*0.6 + 60*0.4 = 42 + 24 = 66
    assert rows[1]["score"] == pytest.approx(66.0)


def test_scorer_values_carol():
    s = CSVScorer(_source(), {"math": 0.6, "english": 0.4})
    rows = list(s.rows())
    # Carol: 50*0.6 + 100*0.4 = 30 + 40 = 70
    assert rows[2]["score"] == pytest.approx(70.0)


def test_scorer_bad_column_raises():
    with pytest.raises(ValueError, match="Unknown columns"):
        CSVScorer(_source(), {"science": 1.0})


def test_scorer_duplicate_score_col_raises():
    src = _FakeSource(["name", "score"], [{"name": "A", "score": "1"}])
    with pytest.raises(ValueError, match="already exists"):
        CSVScorer(src, {"score": 1.0})


def test_scorer_non_numeric_uses_default():
    src = _FakeSource(["name", "math"], [{"name": "X", "math": "n/a"}])
    s = CSVScorer(src, {"math": 2.0}, default=0.0)
    rows = list(s.rows())
    assert rows[0]["score"] == pytest.approx(0.0)


def test_scorer_row_count():
    s = CSVScorer(_source(), {"math": 1.0})
    assert s.row_count == 3
