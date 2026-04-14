import pytest
from csvwrangler.filter import CSVFilter


SAMPLE_ROWS = [
    {"name": "Alice", "age": "30", "city": "New York"},
    {"name": "Bob", "age": "25", "city": "Los Angeles"},
    {"name": "Charlie", "age": "35", "city": "New York"},
    {"name": "Diana", "age": "28", "city": "Chicago"},
    {"name": "Eve", "age": "22", "city": "New York"},
]
HEADERS = ["name", "age", "city"]


def make_filter():
    return CSVFilter(iter(SAMPLE_ROWS), HEADERS)


def test_filter_eq():
    results = list(make_filter().where("city", "==", "New York").apply())
    assert len(results) == 3
    assert all(r["city"] == "New York" for r in results)


def test_filter_ne():
    results = list(make_filter().where("city", "!=", "New York").apply())
    assert len(results) == 2


def test_filter_numeric_gt():
    results = list(make_filter().where("age", ">", 28).apply())
    assert len(results) == 2
    assert {r["name"] for r in results} == {"Alice", "Charlie"}


def test_filter_numeric_lte():
    results = list(make_filter().where("age", "<=", 25).apply())
    assert len(results) == 2
    assert {r["name"] for r in results} == {"Bob", "Eve"}


def test_filter_contains():
    results = list(make_filter().where("city", "contains", "Angeles").apply())
    assert len(results) == 1
    assert results[0]["name"] == "Bob"


def test_filter_startswith():
    results = list(make_filter().where("name", "startswith", "C").apply())
    assert len(results) == 1
    assert results[0]["name"] == "Charlie"


def test_filter_endswith():
    results = list(make_filter().where("name", "endswith", "e").apply())
    names = {r["name"] for r in results}
    assert names == {"Alice", "Charlie", "Eve"}


def test_filter_chained():
    results = list(
        make_filter()
        .where("city", "==", "New York")
        .where("age", ">", 25)
        .apply()
    )
    assert len(results) == 2
    assert {r["name"] for r in results} == {"Alice", "Charlie"}


def test_filter_custom():
    results = list(
        make_filter().custom(lambda row: row["name"].startswith("A") or row["name"].startswith("E")).apply()
    )
    assert {r["name"] for r in results} == {"Alice", "Eve"}


def test_filter_invalid_column():
    with pytest.raises(ValueError, match="Column 'salary' not found"):
        make_filter().where("salary", "==", "50000")


def test_filter_repr():
    f = make_filter().where("city", "==", "Chicago")
    assert "CSVFilter" in repr(f)
    assert "1" in repr(f)
