"""CSVValueSplitter – split rows into named buckets based on a column value."""
from __future__ import annotations
from typing import Dict, Iterable, List


class CSVValueSplitter:
    """Partition source rows into labelled groups by the value of *column*.

    Parameters
    ----------
    source   : object with .headers and .rows
    column   : column name to group on
    """

    def __init__(self, source, column: str) -> None:
        if column not in source.headers:
            raise ValueError(f"Column '{column}' not found in headers: {source.headers}")
        self._source = source
        self._column = column
        self._groups: Dict[str, List[dict]] | None = None

    # ------------------------------------------------------------------
    # public API
    # ------------------------------------------------------------------

    @property
    def headers(self) -> List[str]:
        return list(self._source.headers)

    def _ensure_built(self) -> None:
        if self._groups is not None:
            return
        groups: Dict[str, List[dict]] = {}
        for row in self._source.rows():
            key = row.get(self._column, "")
            groups.setdefault(key, []).append(row)
        self._groups = groups

    def group_keys(self) -> List[str]:
        """Return the distinct values found in *column*, in insertion order."""
        self._ensure_built()
        return list(self._groups.keys())  # type: ignore[union-attr]

    def rows_for(self, key: str) -> List[dict]:
        """Return all rows whose *column* value equals *key*."""
        self._ensure_built()
        return list(self._groups.get(key, []))  # type: ignore[union-attr]

    def all_groups(self) -> Dict[str, List[dict]]:
        """Return a mapping of {value: [rows]} for every distinct value."""
        self._ensure_built()
        return dict(self._groups)  # type: ignore[union-attr]

    def row_count(self) -> int:
        self._ensure_built()
        return sum(len(v) for v in self._groups.values())  # type: ignore[union-attr]

    def __repr__(self) -> str:  # pragma: no cover
        return f"CSVValueSplitter(column={self._column!r})"
