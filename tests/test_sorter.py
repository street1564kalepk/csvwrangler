import io
import pytest
from csvwrangler.reader import CSVReader
from csvwrangler.sorter import CSVSorter


CSV_DATA = """name,age,city
Alice,30,New York
Bob,25,Boston
Carol,35,Chicago
Dave,25,Denver
"""


@pytest.fixture()
def make_sorter():
    def _make(key, reverse=False, numeric=False):
        reader = CSVReader(io.StringIO(CSV_DATA))
        return CSVSorter(reader, key=key, reverse=reverse, numeric=numeric)
    return _make


def test_sorter_headers_unchanged(make_sorter):
    sorter = make_sorter("name")
    assert sorter.headers == ["name", "age", "city"]


def test_sort_ascending_string(make_sorter):
    sorter = make_sorter("name")
    names = [row["name"] for row in sorter.rows]
    assert names == ["Alice", "Bob", "Carol", "Dave"]


def test_sort_descending_string(make_sorter):
    sorter = make_sorter("name", reverse=True)
    names = [row["name"] for row in sorter.rows]
    assert names == ["Dave", "Carol", "Bob", "Alice"]


def test_sort_numeric_ascending(make_sorter):
    sorter = make_sorter("age", numeric=True)
    ages = [row["age"] for row in sorter.rows]
    assert ages == ["25", "25", "30", "35"]


def test_sort_numeric_descending(make_sorter):
    sorter = make_sorter("age", reverse=True, numeric=True)
    ages = [row["age"] for row in sorter.rows]
    assert ages == ["35", "30", "25", "25"]


def test_sort_invalid_key_raises():
    reader = CSVReader(io.StringIO(CSV_DATA))
    with pytest.raises(ValueError, match="Sort key 'missing'"):
        CSVSorter(reader, key="missing")


def test_sort_rows_are_dicts(make_sorter):
    sorter = make_sorter("city")
    for row in sorter.rows:
        assert isinstance(row, dict)
        assert set(row.keys()) == {"name", "age", "city"}


def test_chained_sort(make_sorter):
    """Sort by age numeric asc, then by name asc for ties."""
    first_sorter = make_sorter("age", numeric=True)
    chained = first_sorter.then_sort("name")
    rows = list(chained.rows)
    # Both Bob and Dave are age 25; after secondary sort by name: Bob < Dave
    age_25_names = [r["name"] for r in rows if r["age"] == "25"]
    assert age_25_names == ["Bob", "Dave"]


def test_sort_preserves_all_fields(make_sorter):
    sorter = make_sorter("city")
    rows = list(sorter.rows)
    assert len(rows) == 4
    alice = next(r for r in rows if r["name"] == "Alice")
    assert alice == {"name": "Alice", "age": "30", "city": "New York"}
