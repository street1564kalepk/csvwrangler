import pytest
from csvwrangler.zipper_by_position import CSVPositionZipper


class _FakeSource:
    def __init__(self, hdrs, data):
        self._hdrs = hdrs
        self._data = data

    @property
    def headers(self):
        return list(self._hdrs)

    @property
    def row_count(self):
        return len(self._data)

    def rows(self):
        for row in self._data:
            yield dict(zip(self._hdrs, row))


def _left():
    return _FakeSource(
        ["name", "score"],
        [["Alice", "90"], ["Bob", "80"], ["Carol", "70"]],
    )


def _right():
    return _FakeSource(
        ["dept", "level"],
        [["Eng", "senior"], ["HR", "junior"]],
    )


# ---- headers ----

def test_append_headers():
    z = CSVPositionZipper(_left(), _right(), mode="append")
    assert z.headers == ["name", "score", "dept", "level"]


def test_interleave_headers():
    z = CSVPositionZipper(_left(), _right(), mode="interleave")
    assert z.headers == ["name", "dept", "score", "level"]


def test_alternate_is_same_as_interleave():
    z1 = CSVPositionZipper(_left(), _right(), mode="interleave")
    z2 = CSVPositionZipper(_left(), _right(), mode="alternate")
    assert z1.headers == z2.headers


def test_invalid_mode_raises():
    with pytest.raises(ValueError, match="mode must be one of"):
        CSVPositionZipper(_left(), _right(), mode="bogus")


# ---- row_count ----

def test_row_count_is_max_of_sources():
    z = CSVPositionZipper(_left(), _right())
    assert z.row_count == 3


# ---- rows: append mode ----

def test_append_rows_count():
    z = CSVPositionZipper(_left(), _right())
    assert len(list(z.rows())) == 3


def test_append_first_row_all_fields():
    z = CSVPositionZipper(_left(), _right())
    row = list(z.rows())[0]
    assert row["name"] == "Alice"
    assert row["score"] == "90"
    assert row["dept"] == "Eng"
    assert row["level"] == "senior"


def test_append_short_right_pads_empty():
    z = CSVPositionZipper(_left(), _right())
    rows = list(z.rows())
    # third row: right source exhausted
    assert rows[2]["dept"] == ""
    assert rows[2]["level"] == ""
    assert rows[2]["name"] == "Carol"


# ---- rows: interleave mode ----

def test_interleave_first_row_order():
    z = CSVPositionZipper(_left(), _right(), mode="interleave")
    row = list(z.rows())[0]
    assert row["name"] == "Alice"
    assert row["dept"] == "Eng"
    assert row["score"] == "90"
    assert row["level"] == "senior"


def test_interleave_padding_on_short_right():
    z = CSVPositionZipper(_left(), _right(), mode="interleave")
    rows = list(z.rows())
    assert rows[2]["dept"] == ""
