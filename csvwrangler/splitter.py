"""CSVSplitter — splits rows into groups based on a column value."""
from __future__ import annotations
from typing import Iterator, Dict, List


class CSVSplitter:
    """Groups rows by the unique values of a given column.

    Parameters
    ----------
    source:
        Any object exposing ``headers`` (list[str]) and ``rows()``
        (iterable of dict).
    column:
        The column whose values determine the group each row belongs to.
    """

    def __init__(self, source, column: str) -> None:
        if not hasattr(source, "headers") or not hasattr(source, "rows"):
            raise TypeError("source must expose 'headers' and 'rows'")
        self._source = source
        self._column = column
        if column not in source.headers:
            raise ValueError(
                f"Column '{column}' not found in headers: {source.headers}"
            )

    @property
    def headers(self) -> List[str]:
        return self._source.headers

    def groups(self) -> Dict[str, List[dict]]:
        """Return a mapping of group-key -> list of row dicts."""
        result: Dict[str, List[dict]] = {}
        for row in self._source.rows():
            key = row.get(self._column, "")
            result.setdefault(key, []).append(row)
        return result

    def rows(self) -> Iterator[dict]:
        """Yield all rows (order preserved, same as source)."""
        yield from self._source.rows()

    def row_count(self) -> int:
        """Total number of rows across all groups."""
        return sum(1 for _ in self._source.rows())

    def group_count(self) -> int:
        """Number of distinct groups."""
        return len(self.groups())

    def __repr__(self) -> str:  # pragma: no cover
        return f"CSVSplitter(column={self._column!r})"
