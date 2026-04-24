"""Tests for CSVRankClipper."""
import pytest
from csvwrangler.clipper_by_rank import CSVRankClipper


class _FakeSource:
    def __init__(self, headers, rows):
        self._headers = headers
        self._rows = rows

    @property
    def headers(self):
        return list(self._headers)

    def rows(self):
        yield from self._rows


def _source():
    return _FakeSource(
        ["name", "score"],
        [
            {"name": "Alice", "score": "85"},
            {"name": "Bob", "score": "92"},
            {"name": "Carol", "score": "78"},
            {"name": "Dave", "score": "95"},
            {"name": "Eve", "score": "60"},
        ],
    )


def test_headers_unchanged():
    clipper = CSVRankClipper(_source(), "score", 3)
    assert clipper.headers == ["name", "score"]


def test_top_n_returns_correct_count():
    clipper = CSVRankClipper(_source(), "score", 3, direction="top")
    result = list(clipper.rows())
    assert len(result) == 3


def test_top_n_returns_highest_values():
    clipper = CSVRankClipper(_source(), "score", 2, direction="top")
    result = list(clipper.rows())
    names = [r["name"] for r in result]
    assert "Dave" in names
    assert "Bob" in names


def test_bottom_n_returns_lowest_values():
    clipper = CSVRankClipper(_source(), "score", 2, direction="bottom")
    result = list(clipper.rows())
    names = [r["name"] for r in result]
    assert "Eve" in names
    assert "Carol" in names


def test_n_larger_than_rows_returns_all():
    clipper = CSVRankClipper(_source(), "score", 100)
    result = list(clipper.rows())
    assert len(result) == 5


def test_n_zero_returns_empty():
    clipper = CSVRankClipper(_source(), "score", 0)
    result = list(clipper.rows())
    assert result == []


def test_row_count_property():
    clipper = CSVRankClipper(_source(), "score", 3)
    assert clipper.row_count == 3


def test_invalid_direction_raises():
    with pytest.raises(ValueError, match="direction"):
        CSVRankClipper(_source(), "score", 3, direction="middle")


def test_negative_n_raises():
    with pytest.raises(ValueError, match="non-negative"):
        CSVRankClipper(_source(), "score", -1)


def test_non_numeric_column_raises():
    src = _FakeSource(
        ["name", "score"],
        [{"name": "Alice", "score": "abc"}],
    )
    clipper = CSVRankClipper(src, "score", 1)
    with pytest.raises(ValueError, match="non-numeric"):
        list(clipper.rows())
