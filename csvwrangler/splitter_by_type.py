"""Split rows into groups based on the inferred type of a column's value.

Groups: 'int', 'float', 'bool', 'empty', 'string'.
"""

from __future__ import annotations

from typing import Iterator


def _infer_type(value: str) -> str:
    if value.strip() == "":
        return "empty"
    if value.strip().lower() in ("true", "false"):
        return "bool"
    try:
        int(value)
        return "int"
    except ValueError:
        pass
    try:
        float(value)
        return "float"
    except ValueError:
        pass
    return "string"


class CSVTypeSplitter:
    """Split source rows into typed buckets based on a single column."""

    def __init__(self, source, column: str) -> None:
        self._source = source
        self._column = column
        self._built: dict[str, list[dict]] = {}
        self._done = False

    @property
    def headers(self) -> list[str]:
        return list(self._source.headers)

    def _ensure_built(self) -> None:
        if self._done:
            return
        col = self._column
        if col not in self._source.headers:
            raise KeyError(f"Column '{col}' not found in headers")
        buckets: dict[str, list[dict]] = {}
        for row in self._source.rows():
            key = _infer_type(row.get(col, ""))
            buckets.setdefault(key, []).append(row)
        self._built = buckets
        self._done = True

    @property
    def group_keys(self) -> list[str]:
        self._ensure_built()
        return sorted(self._built.keys())

    def rows(self, group: str) -> Iterator[dict]:
        self._ensure_built()
        yield from self._built.get(group, [])

    def row_count(self, group: str) -> int:
        self._ensure_built()
        return len(self._built.get(group, []))
