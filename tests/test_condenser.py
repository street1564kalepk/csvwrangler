import pytest
from csvwrangler.condenser import CSVCondenser


class _FakeSource:
    def __init__(self, headers, data):
        self._headers = headers
        self._data = data

    @property
    def headers(self):
        return self._headers

    def rows(self):
        for r in self._data:
            yield dict(zip(self._headers, r))


def _source():
    return _FakeSource(
        ["dept", "name", "score"],
        [
            ("eng", "alice", "10"),
            ("eng", "bob", "20"),
            ("hr", "carol", "5"),
            ("hr", "dave", "15"),
            ("hr", "eve", "10"),
            ("fin", "frank", "30"),
        ],
    )


def test_headers_unchanged():
    c = CSVCondenser(_source(), key="dept", agg="first")
    assert c.headers == ["dept", "name", "score"]


def test_invalid_agg_raises():
    with pytest.raises(ValueError, match="agg must be"):
        CSVCondenser(_source(), key="dept", agg="median")


def test_invalid_key_raises():
    with pytest.raises(ValueError, match="Key column"):
        CSVCondenser(_source(), key="missing", agg="first")


def test_agg_first():
    rows = list(CSVCondenser(_source(), key="dept", agg="first").rows())
    assert len(rows) == 3
    assert rows[0] == {"dept": "eng", "name": "alice", "score": "10"}
    assert rows[1] == {"dept": "hr", "name": "carol", "score": "5"}


def test_agg_last():
    rows = list(CSVCondenser(_source(), key="dept", agg="last").rows())
    assert rows[0]["name"] == "bob"
    assert rows[1]["name"] == "eve"


def test_agg_count():
    rows = list(CSVCondenser(_source(), key="dept", agg="count").rows())
    assert rows[0]["score"] == "2"
    assert rows[1]["score"] == "3"
    assert rows[2]["score"] == "1"


def test_agg_sum():
    rows = list(CSVCondenser(_source(), key="dept", agg="sum").rows())
    assert rows[0]["score"] == "30.0"
    assert rows[1]["score"] == "30.0"


def test_agg_join():
    rows = list(CSVCondenser(_source(), key="dept", agg="join").rows())
    assert rows[0]["name"] == "alice|bob"
    assert rows[1]["name"] == "carol|dave|eve"


def test_agg_join_custom_sep():
    rows = list(CSVCondenser(_source(), key="dept", agg="join", sep=",").rows())
    assert rows[1]["name"] == "carol,dave,eve"


def test_row_count():
    c = CSVCondenser(_source(), key="dept", agg="first")
    assert c.row_count == 3
