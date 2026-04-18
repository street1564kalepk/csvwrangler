"""CSVDropper – drop columns from a source by name."""
from __future__ import annotations
from typing import Iterable


class CSVDropper:
    """Removes named columns from every row of *source*."""

    def __init__(self, source, columns: list[str]) -> None:
        if not columns:
            raise ValueError("columns must not be empty")
        src_headers = list(source.headers)
        unknown = [c for c in columns if c not in src_headers]
        if unknown:
            raise ValueError(f"Unknown columns: {unknown}")
        self._source = source
        self._drop = set(columns)
        self._headers = [h for h in src_headers if h not in self._drop]

    @property
    def headers(self) -> list[str]:
        return list(self._headers)

    def rows(self) -> Iterable[dict]:
        for row in self._source.rows():
            yield {k: v for k, v in row.items() if k not in self._drop}

    @property
    def row_count(self) -> int:
        return sum(1 for _ in self.rows())

    def __repr__(self) -> str:  # pragma: no cover
        return f"CSVDropper(drop={sorted(self._drop)}, headers={self._headers})"
