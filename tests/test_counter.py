import pytest
from csvwrangler.counter import CSVCounter


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
        headers=["name", "city"],
        data=[
            {"name": "Alice", "city": "London"},
            {"name": "Bob", "city": "Paris"},
            {"name": "Carol", "city": "London"},
            {"name": "Dave", "city": "London"},
            {"name": "Eve", "city": "Paris"},
            {"name": "Frank", "city": "Berlin"},
        ],
    )


def test_counter_headers():
    c = CSVCounter(_source(), "city")
    assert c.headers == ["city", "count"]


def test_counter_row_count():
    c = CSVCounter(_source(), "city")
    assert c.row_count == 3  # London, Paris, Berlin


def test_counter_counts_correct():
    c = CSVCounter(_source(), "city")
    result = {r["city"]: r["count"] for r in c.rows()}
    assert result == {"London": "3", "Paris": "2", "Berlin": "1"}


def test_counter_sort_by_count_descending_default():
    c = CSVCounter(_source(), "city", sort_by="count", ascending=False)
    rows = list(c.rows())
    assert rows[0]["city"] == "London"
    assert rows[-1]["city"] == "Berlin"


def test_counter_sort_by_count_ascending():
    c = CSVCounter(_source(), "city", sort_by="count", ascending=True)
    rows = list(c.rows())
    assert rows[0]["city"] == "Berlin"
    assert rows[-1]["city"] == "London"


def test_counter_sort_by_value():
    c = CSVCounter(_source(), "city", sort_by="value", ascending=True)
    rows = list(c.rows())
    cities = [r["city"] for r in rows]
    assert cities == sorted(cities)


def test_counter_invalid_column():
    with pytest.raises(ValueError, match="not found"):
        CSVCounter(_source(), "country")


def test_counter_invalid_sort_by():
    with pytest.raises(ValueError, match="sort_by"):
        CSVCounter(_source(), "city", sort_by="invalid")


def test_counter_repr():
    c = CSVCounter(_source(), "city")
    assert "CSVCounter" in repr(c)
    assert "city" in repr(c)
