"""Deduplication support for CSV pipelines."""
from typing import Iterable, Iterator, List, Optional


class CSVDeduplicator:
    """Removes duplicate rows from a CSV source, optionally keyed on specific columns."""

    def __init__(self, source, key_columns: Optional[List[str]] = None):
        """
        Parameters
        ----------
        source:
            Any object exposing ``headers`` and ``rows()``.
        key_columns:
            Columns used to determine uniqueness.  When *None* all columns are
            used as the key.
        """
        self._source = source
        self._key_columns = key_columns

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    @property
    def headers(self) -> List[str]:
        """Pass headers through unchanged."""
        return self._source.headers

    def rows(self) -> Iterator[dict]:
        """Yield rows, skipping any that have already been seen."""
        key_cols = self._key_columns if self._key_columns is not None else self.headers
        seen: set = set()
        for row in self._source.rows():
            key = tuple(row.get(col, "") for col in key_cols)
            if key in seen:
                continue
            seen.add(key)
            yield row

    def row_count(self) -> int:
        """Return the number of deduplicated rows (materialises the iterator)."""
        return sum(1 for _ in self.rows())

    # ------------------------------------------------------------------
    # Chaining helpers
    # ------------------------------------------------------------------

    def then_sort(self, column: str, reverse: bool = False):
        """Convenience: attach a CSVSorter after deduplication."""
        from csvwrangler.sorter import CSVSorter
        return CSVSorter(self, column, reverse=reverse)

    def where(self, column: str, op: str, value: str):
        """Convenience: attach a CSVFilter after deduplication."""
        from csvwrangler.filter import CSVFilter
        return CSVFilter(self, column, op, value)

    def __repr__(self) -> str:  # pragma: no cover
        keys = self._key_columns or "all"
        return f"CSVDeduplicator(key_columns={keys!r})"
