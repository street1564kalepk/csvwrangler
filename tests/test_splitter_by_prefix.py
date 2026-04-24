"""Tests for CSVPrefixSplitter."""

from __future__ import annotations

from typing import Iterator, List

import pytest

from csvwrangler.splitter_by_prefix import CSVPrefixSplitter


class _FakeSource:
    def __init__(self, headers: List[str], data: List[dict]) -> None:
        self._headers = headers
        self._data = data

    @property
    def headers(self) -> List[str]:
        return list(self._headers)

    @property
    def rows(self) -> Iterator[dict]:
        yield from self._data


def _source() -> _FakeSource:
    return _FakeSource(
        headers=["id", "code", "value"],
        data=[
            {"id": "1", "code": "AA-001", "value": "10"},
            {"id": "2", "code": "AA-002", "value": "20"},
            {"id": "3", "code": "BB-001", "value": "30"},
            {"id": "4", "code": "BB-002", "value": "40"},
            {"id": "5", "code": "CC-001", "value": "50"},
            {"id": "6", "code": "ZZ-999", "value": "60"},
        ],
    )


def test_headers_unchanged() -> None:
    sp = CSVPrefixSplitter(_source(), "code", ["AA", "BB"])
    assert sp.headers == ["id", "code", "value"]


def test_group_keys_include_prefixes_and_other() -> None:
    sp = CSVPrefixSplitter(_source(), "code", ["AA", "BB"])
    assert set(sp.group_keys) == {"AA", "BB", "__other__"}


def test_group_aa_rows() -> None:
    sp = CSVPrefixSplitter(_source(), "code", ["AA", "BB"])
    rows = sp.group("AA")
    assert len(rows) == 2
    assert all(r["code"].startswith("AA") for r in rows)


def test_group_bb_rows() -> None:
    sp = CSVPrefixSplitter(_source(), "code", ["AA", "BB"])
    rows = sp.group("BB")
    assert len(rows) == 2
    assert all(r["code"].startswith("BB") for r in rows)


def test_other_group_captures_unmatched() -> None:
    sp = CSVPrefixSplitter(_source(), "code", ["AA", "BB"])
    other = sp.group("__other__")
    codes = {r["code"] for r in other}
    assert "CC-001" in codes
    assert "ZZ-999" in codes


def test_group_count() -> None:
    sp = CSVPrefixSplitter(_source(), "code", ["AA", "BB", "CC"])
    assert sp.group_count == 4  # AA, BB, CC, __other__


def test_all_groups_returns_all() -> None:
    sp = CSVPrefixSplitter(_source(), "code", ["AA"])
    groups = sp.all_groups()
    assert "AA" in groups
    assert "__other__" in groups
    total = sum(len(v) for v in groups.values())
    assert total == 6


def test_case_insensitive_matching() -> None:
    src = _FakeSource(
        headers=["id", "code"],
        data=[
            {"id": "1", "code": "aa-001"},
            {"id": "2", "code": "BB-002"},
        ],
    )
    sp = CSVPrefixSplitter(src, "code", ["AA"], case_sensitive=False)
    assert len(sp.group("AA")) == 1
    assert sp.group("AA")[0]["id"] == "1"


def test_invalid_column_raises() -> None:
    with pytest.raises(ValueError, match="not found"):
        CSVPrefixSplitter(_source(), "nonexistent", ["AA"])


def test_empty_prefixes_raises() -> None:
    with pytest.raises(ValueError, match="At least one prefix"):
        CSVPrefixSplitter(_source(), "code", [])


def test_unknown_group_key_raises() -> None:
    sp = CSVPrefixSplitter(_source(), "code", ["AA"])
    with pytest.raises(KeyError):
        sp.group("MISSING")
