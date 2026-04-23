"""Integration tests for Pipeline.coalesce()."""
import io
from csvwrangler.pipeline import Pipeline
from csvwrangler.pipeline_coalescer_patch import _patch

_patch(Pipeline)


_CSV = (
    "id,primary,secondary,tertiary\n"
    "1,Alice,,\n"
    "2,,Bob,\n"
    "3,,,Carol\n"
    "4,,,\n"
    "5,Dave,Eve,Frank\n"
)


def pipeline():
    return Pipeline.from_string(_CSV)


def _parse(csv_str: str) -> list:
    lines = csv_str.strip().splitlines()
    headers = lines[0].split(",")
    return [
        dict(zip(headers, line.split(",")))
        for line in lines[1:]
    ]


def test_coalesce_method_exists():
    assert callable(getattr(pipeline(), "coalesce", None))


def test_coalesce_headers_appends_target():
    p = pipeline().coalesce(["primary", "secondary", "tertiary"], "name")
    headers = p._source.headers
    assert "name" in headers
    assert headers.index("name") == len(headers) - 1


def test_coalesce_values_first_non_empty():
    out = pipeline().coalesce(["primary", "secondary", "tertiary"], "name").to_string()
    rows = _parse(out)
    assert rows[0]["name"] == "Alice"
    assert rows[1]["name"] == "Bob"
    assert rows[2]["name"] == "Carol"


def test_coalesce_empty_string_when_all_empty():
    out = pipeline().coalesce(["primary", "secondary", "tertiary"], "name").to_string()
    rows = _parse(out)
    assert rows[3]["name"] == ""


def test_coalesce_first_wins_when_multiple_non_empty():
    out = pipeline().coalesce(["primary", "secondary", "tertiary"], "name").to_string()
    rows = _parse(out)
    assert rows[4]["name"] == "Dave"


def test_coalesce_chainable_with_select():
    out = (
        pipeline()
        .coalesce(["primary", "secondary", "tertiary"], "name")
        .select(["id", "name"])
        .to_string()
    )
    rows = _parse(out)
    assert list(rows[0].keys()) == ["id", "name"]
    assert rows[0]["name"] == "Alice"
