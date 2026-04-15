import pytest
from csvwrangler.aggregator import CSVAggregator


class _FakeSource:
    def __init__(self, headers, data):
        self._headers = headers
        self._data = data

    @property
    def headers(self):
        return self._headers

    def rows(self):
        return iter(self._data)


SALES_DATA = [
    {"region": "North", "product": "A", "amount": "100", "qty": "2"},
    {"region": "North", "product": "A", "amount": "200", "qty": "3"},
    {"region": "South", "product": "B", "amount": "150", "qty": "1"},
    {"region": "South", "product": "B", "amount": "50",  "qty": "4"},
    {"region": "North", "product": "B", "amount": "80",  "qty": "1"},
]


@pytest.fixture
def source():
    return _FakeSource(["region", "product", "amount", "qty"], SALES_DATA)


def make_agg(source, group_by, aggregations):
    return CSVAggregator(source, group_by, aggregations)


def test_headers(source):
    agg = make_agg(source, ["region"], {"total": "sum:amount"})
    assert agg.headers == ["region", "total"]


def test_headers_multiple_group_and_agg(source):
    agg = make_agg(source, ["region", "product"], {"cnt": "count:qty", "s": "sum:amount"})
    assert agg.headers == ["region", "product", "cnt", "s"]


def test_count(source):
    agg = make_agg(source, ["region"], {"cnt": "count:amount"})
    result = {r["region"]: r["cnt"] for r in agg.rows()}
    assert result["North"] == 3
    assert result["South"] == 2


def test_sum(source):
    agg = make_agg(source, ["region"], {"total": "sum:amount"})
    result = {r["region"]: r["total"] for r in agg.rows()}
    assert result["North"] == pytest.approx(380.0)
    assert result["South"] == pytest.approx(200.0)


def test_min(source):
    agg = make_agg(source, ["region"], {"min_amt": "min:amount"})
    result = {r["region"]: r["min_amt"] for r in agg.rows()}
    assert result["North"] == pytest.approx(80.0)
    assert result["South"] == pytest.approx(50.0)


def test_max(source):
    agg = make_agg(source, ["region"], {"max_amt": "max:amount"})
    result = {r["region"]: r["max_amt"] for r in agg.rows()}
    assert result["North"] == pytest.approx(200.0)
    assert result["South"] == pytest.approx(150.0)


def test_mean(source):
    agg = make_agg(source, ["region"], {"avg": "mean:amount"})
    result = {r["region"]: r["avg"] for r in agg.rows()}
    assert result["North"] == pytest.approx(380.0 / 3)
    assert result["South"] == pytest.approx(100.0)


def test_row_count(source):
    agg = make_agg(source, ["region"], {"cnt": "count:amount"})
    assert agg.row_count() == 2


def test_invalid_group_by_column(source):
    with pytest.raises(ValueError, match="Group-by column"):
        make_agg(source, ["nonexistent"], {"cnt": "count:amount"})


def test_invalid_agg_column(source):
    with pytest.raises(ValueError, match="Aggregation column"):
        make_agg(source, ["region"], {"x": "sum:nonexistent"})


def test_invalid_agg_op(source):
    with pytest.raises(ValueError, match="Unsupported aggregation op"):
        make_agg(source, ["region"], {"x": "median:amount"})


def test_invalid_agg_spec_format(source):
    with pytest.raises(ValueError, match="must be 'op:column'"):
        make_agg(source, ["region"], {"x": "sum"})
