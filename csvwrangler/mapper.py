"""CSVMapper – apply a lookup-table mapping to one or more columns.

For each specified column the mapper replaces values found in the
provided mapping dict; values not present in the mapping are left
unchanged unless *default* is supplied, in which case unmatched
values are replaced with *default*.
"""

from __future__ import annotations

from typing import Any, Dict, Iterable, Iterator, List, Optional


class CSVMapper:
    """Wrap a source and apply a value-mapping to selected columns."""

    def __init__(
        self,
        source,
        columns: List[str],
        mapping: Dict[str, Any],
        default: Optional[Any] = None,
    ) -> None:
        if not columns:
            raise ValueError("columns must not be empty")
        if not isinstance(mapping, dict):
            raise TypeError("mapping must be a dict")
        self._source = source
        self._columns = columns
        self._mapping = mapping
        self._default = default

    # ------------------------------------------------------------------
    # public interface
    # ------------------------------------------------------------------

    @property
    def headers(self) -> List[str]:
        """Pass-through headers – mapping never changes the schema."""
        return self._source.headers

    def rows(self) -> Iterator[dict]:
        """Yield rows with mapped values applied."""
        for row in self._source.rows():
            yield self._map_row(row)

    @property
    def row_count(self) -> int:
        return sum(1 for _ in self.rows())

    # ------------------------------------------------------------------
    # internals
    # ------------------------------------------------------------------

    def _map_row(self, row: dict) -> dict:
        out = dict(row)
        for col in self._columns:
            if col not in out:
                continue
            original = out[col]
            if original in self._mapping:
                out[col] = self._mapping[original]
            elif self._default is not None:
                out[col] = self._default
        return out
