"""Tests for CSVSequencer."""

import pytest
from csvwrangler.sequencer import CSVSequencer


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
        ["name", "dept"],
        [
            {"name": "Alice", "dept": "Eng"},
            {"name": "Bob", "dept": "HR"},
            {"name": "Carol", "dept": "Eng"},
        ],
    )


def test_headers_first():
    s = CSVSequencer(_source(), column="seq")
    assert s.headers == ["seq", "name", "dept"]


def test_headers_last():
    s = CSVSequencer(_source(), column="id", position="last")
    assert s.headers == ["name", "dept", "id"]


def test_default_sequence_values():
    rows = list(CSVSequencer(_source()).rows())
    assert [r["seq"] for r in rows] == ["1", "2", "3"]


def test_custom_start_and_step():
    rows = list(CSVSequencer(_source(), start=10, step=10).rows())
    assert [r["seq"] for r in rows] == ["10", "20", "30"]


def test_position_last_values():
    rows = list(CSVSequencer(_source(), column="n", position="last").rows())
    assert rows[0] == {"name": "Alice", "dept": "Eng", "n": "1"}


def test_data_preserved():
    rows = list(CSVSequencer(_source()).rows())
    assert rows[1]["name"] == "Bob"
    assert rows[1]["dept"] == "HR"


def test_row_count():
    s = CSVSequencer(_source())
    assert s.row_count == 3


def test_invalid_position():
    with pytest.raises(ValueError, match="position"):
        CSVSequencer(_source(), position="middle")


def test_custom_column_name():
    s = CSVSequencer(_source(), column="row_num")
    assert s.headers[0] == "row_num"
