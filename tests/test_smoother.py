"""Tests for CSVSmoother."""
import pytest
from csvwrangler.smoother import CSVSmoother


class _FakeSource:
    def __init__(self, headers, data):
        self._headers = headers
        self._data = data

    @property
    def headers(self):
        return list(self._headers)

    def rows(self):
        return iter([dict(zip(self._headers, row)) for row in self._data])


def _source():
    return _FakeSource(
        ["name", "score"],
        [
            ["alice", "10"],
            ["bob", "20"],
            ["carol", "30"],
            ["dave", "40"],
            ["eve", "50"],
        ],
    )


# ---------------------------------------------------------------------------
# headers
# ---------------------------------------------------------------------------

def test_headers_unchanged_when_no_suffix():
    s = CSVSmoother(_source(), columns=["score"], window=3)
    assert s.headers == ["name", "score"]


def test_headers_appended_when_suffix_given():
    s = CSVSmoother(_source(), columns=["score"], window=3, target_suffix="_smooth")
    assert s.headers == ["name", "score", "score_smooth"]


# ---------------------------------------------------------------------------
# mean smoothing
# ---------------------------------------------------------------------------

def test_mean_first_row_equals_itself():
    s = CSVSmoother(_source(), columns=["score"], window=3)
    first = next(iter(s.rows()))
    assert first["score"] == "10"


def test_mean_second_row_average_of_two():
    s = CSVSmoother(_source(), columns=["score"], window=3)
    rows = list(s.rows())
    assert rows[1]["score"] == "15"


def test_mean_third_row_average_of_three():
    s = CSVSmoother(_source(), columns=["score"], window=3)
    rows = list(s.rows())
    assert rows[2]["score"] == "20"


def test_mean_fourth_row_window_slides():
    s = CSVSmoother(_source(), columns=["score"], window=3)
    rows = list(s.rows())
    # window=[20,30,40] -> mean=30
    assert rows[3]["score"] == "30"


# ---------------------------------------------------------------------------
# median smoothing
# ---------------------------------------------------------------------------

def test_median_third_row():
    s = CSVSmoother(_source(), columns=["score"], window=3, method="median")
    rows = list(s.rows())
    # window=[10,20,30] -> median=20
    assert rows[2]["score"] == "20"


def test_median_fourth_row():
    s = CSVSmoother(_source(), columns=["score"], window=3, method="median")
    rows = list(s.rows())
    # window=[20,30,40] -> median=30
    assert rows[3]["score"] == "30"


# ---------------------------------------------------------------------------
# suffix / target column
# ---------------------------------------------------------------------------

def test_suffix_original_column_preserved():
    s = CSVSmoother(_source(), columns=["score"], window=3, target_suffix="_sm")
    rows = list(s.rows())
    assert rows[0]["score"] == "10"  # original untouched


def test_suffix_new_column_has_smoothed_value():
    s = CSVSmoother(_source(), columns=["score"], window=3, target_suffix="_sm")
    rows = list(s.rows())
    assert rows[2]["score_sm"] == "20"


# ---------------------------------------------------------------------------
# validation
# ---------------------------------------------------------------------------

def test_invalid_window_raises():
    with pytest.raises(ValueError, match="window"):
        CSVSmoother(_source(), columns=["score"], window=0)


def test_invalid_method_raises():
    with pytest.raises(ValueError, match="method"):
        CSVSmoother(_source(), columns=["score"], method="mode")


def test_non_numeric_value_yields_empty():
    src = _FakeSource(["name", "score"], [["alice", "n/a"]])
    s = CSVSmoother(src, columns=["score"], window=3)
    rows = list(s.rows())
    assert rows[0]["score"] == ""


def test_row_count():
    s = CSVSmoother(_source(), columns=["score"], window=3)
    assert s.row_count == 5
