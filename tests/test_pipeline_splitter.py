"""Integration tests: Pipeline.split_by via pipeline DSL."""
import io
import pytest
from csvwrangler.pipeline import Pipeline


_CSV_TEXT = """dept,name,salary
eng,Alice,90000
hr,Bob,60000
eng,Carol,95000
hr,Dave,62000
eng,Eve,88000
"""


@pytest.fixture()
def pipeline():
    return Pipeline.from_string(_CSV_TEXT)


def _parse(text: str):
    return Pipeline.from_string(text)


def test_split_by_group_count(pipeline):
    sp = pipeline.split_by("dept")
    assert sp.group_count() == 2


def test_split_by_group_keys(pipeline):
    sp = pipeline.split_by("dept")
    assert set(sp.groups().keys()) == {"eng", "hr"}


def test_split_by_eng_count(pipeline):
    sp = pipeline.split_by("dept")
    assert len(sp.groups()["eng"]) == 3


def test_split_by_hr_count(pipeline):
    sp = pipeline.split_by("dept")
    assert len(sp.groups()["hr"]) == 2


def test_split_by_headers_preserved(pipeline):
    sp = pipeline.split_by("dept")
    assert sp.headers == ["dept", "name", "salary"]


def test_split_by_invalid_column_raises(pipeline):
    with pytest.raises(ValueError):
        pipeline.split_by("nonexistent")


def test_split_after_filter():
    sp = _parse(_CSV_TEXT).where("dept", "eq", "eng").split_by("dept")
    assert sp.group_count() == 1
    assert len(sp.groups()["eng"]) == 3
