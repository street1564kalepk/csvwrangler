import io
import pytest
from csvwrangler.reader import CSVReader
from csvwrangler.transform import CSVTransform


SAMPLE_CSV = """name,age,city
Alice,30,New York
Bob,25,Los Angeles
Carol,35,Chicago
"""


@pytest.fixture
def make_transform():
    def _make():
        source = CSVReader(io.StringIO(SAMPLE_CSV))
        return CSVTransform(source)
    return _make


def test_transform_headers_unchanged(make_transform):
    t = make_transform()
    assert t.headers == ["name", "age", "city"]


def test_rename_column(make_transform):
    t = make_transform().rename("city", "location")
    assert "location" in t.headers
    assert "city" not in t.headers


def test_rename_preserves_data(make_transform):
    t = make_transform().rename("name", "full_name")
    rows = list(t.rows())
    assert rows[0]["full_name"] == "Alice"
    assert "name" not in rows[0]


def test_add_computed_column(make_transform):
    t = make_transform().add_column(
        "name_upper", lambda row: row["name"].upper()
    )
    assert "name_upper" in t.headers
    rows = list(t.rows())
    assert rows[0]["name_upper"] == "ALICE"
    assert rows[1]["name_upper"] == "BOB"


def test_apply_transform_to_column(make_transform):
    t = make_transform().apply("age", lambda v: str(int(v) + 1))
    rows = list(t.rows())
    assert rows[0]["age"] == "31"
    assert rows[1]["age"] == "26"


def test_apply_and_rename_combined(make_transform):
    t = (
        make_transform()
        .apply("name", str.lower)
        .rename("name", "username")
    )
    rows = list(t.rows())
    assert rows[0]["username"] == "alice"
    assert "name" not in rows[0]


def test_multiple_transforms(make_transform):
    t = (
        make_transform()
        .rename("city", "location")
        .add_column("summary", lambda row: f"{row['name']} from {row['city']}")
        .apply("age", lambda v: v.zfill(3))
    )
    rows = list(t.rows())
    assert rows[0]["location"] == "New York"
    assert rows[0]["summary"] == "Alice from New York"
    assert rows[0]["age"] == "030"


def test_repr(make_transform):
    t = make_transform().rename("age", "years").add_column("x", lambda r: "1")
    r = repr(t)
    assert "CSVTransform" in r
    assert "years" in r
    assert "x" in r
