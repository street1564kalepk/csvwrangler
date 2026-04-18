"""Tests for CSVCrosser."""
import pytest
from csvwrangler.crosser import CSVCrosser


class _FakeSource:
    def __init__(self, hdrs, data):
        self._headers = hdrs
        self._data = data

    def headers(self):
        return list(self._headers)

    def rows(self):
        for r in self._data:
            yield dict(zip(self._headers, r))


def _left():
    return _FakeSource(["id", "name"], [("1", "Alice"), ("2", "Bob")])


def _right():
    return _FakeSource(["color"], [("red",), ("blue",)])


def test_crosser_headers():
    c = CSVCrosser(_left(), _right())
    assert c.headers() == ["left_id", "left_name", "right_color"]


def test_crosser_row_count():
    c = CSVCrosser(_left(), _right())
    assert c.row_count() == 4


def test_crosser_combinations():
    c = CSVCrosser(_left(), _right())
    result = list(c.rows())
    names = [r["left_name"] for r in result]
    colors = [r["right_color"] for r in result]
    assert names == ["Alice", "Alice", "Bob", "Bob"]
    assert colors == ["red", "blue", "red", "blue"]


def test_crosser_custom_prefix():
    c = CSVCrosser(_left(), _right(), left_prefix="L_", right_prefix="R_")
    assert "L_id" in c.headers()
    assert "R_color" in c.headers()


def test_crosser_single_row_each():
    left = _FakeSource(["x"], [("1",)])
    right = _FakeSource(["y"], [("a",)])
    c = CSVCrosser(left, right)
    rows = list(c.rows())
    assert len(rows) == 1
    assert rows[0] == {"left_x": "1", "right_y": "a"}
