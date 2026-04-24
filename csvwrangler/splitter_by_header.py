"""CSVHeaderSplitter – split a CSV source into groups based on a header prefix.

Each unique prefix (the part before a configurable separator, default '_') found
in the column names becomes a group.  Columns that have no separator are placed
in a catch-all group whose key is the empty string ``''``.

Example
-------
    Given headers: id, sales_q1, sales_q2, cost_q1, cost_q2
    With separator='_', prefix groups are:
        ''       -> [id]
        'sales'  -> [sales_q1, sales_q2]
        'cost'   -> [cost_q1, cost_q2]
"""

from __future__ import annotations

from typing import Dict, Iterator, List


class CSVHeaderSplitter:
    """Split a source into named sub-sources keyed by column-name prefix."""

    def __init__(self, source, separator: str = "_") -> None:
        if not separator:
            raise ValueError("separator must be a non-empty string")
        self._source = source
        self._sep = separator
        self._groups: Dict[str, List[str]] | None = None

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _ensure_built(self) -> None:
        if self._groups is not None:
            return
        groups: Dict[str, List[str]] = {}
        for col in self._source.headers:
            prefix = col.split(self._sep, 1)[0] if self._sep in col else ""
            groups.setdefault(prefix, []).append(col)
        self._groups = groups

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    @property
    def headers(self) -> List[str]:
        return self._source.headers

    @property
    def group_keys(self) -> List[str]:
        self._ensure_built()
        return list(self._groups.keys())  # type: ignore[index]

    @property
    def group_count(self) -> int:
        self._ensure_built()
        return len(self._groups)  # type: ignore[arg-type]

    def group_headers(self, key: str) -> List[str]:
        """Return the list of column names that belong to *key*."""
        self._ensure_built()
        if key not in self._groups:  # type: ignore[operator]
            raise KeyError(f"group {key!r} not found")
        return list(self._groups[key])  # type: ignore[index]

    def rows(self, key: str) -> Iterator[dict]:
        """Yield rows projected to the columns in group *key*."""
        cols = self.group_headers(key)
        for row in self._source.rows():
            yield {c: row[c] for c in cols}
