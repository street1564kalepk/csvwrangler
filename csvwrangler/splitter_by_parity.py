"""CSVParitySplitter – splits rows into two groups: even-indexed and odd-indexed."""

from __future__ import annotations
from typing import Iterator, List, Dict


class CSVParitySplitter:
    """Splits a source into 'even' and 'odd' row groups (0-based index)."""

    def __init__(self, source) -> None:
        self._source = source
        self._even: List[Dict[str, str]] = []
        self._odd: List[Dict[str, str]] = []
        self._built = False

    @property
    def headers(self) -> List[str]:
        return self._source.headers

    def _ensure_built(self) -> None:
        if self._built:
            return
        for idx, row in enumerate(self._source.rows()):
            if idx % 2 == 0:
                self._even.append(row)
            else:
                self._odd.append(row)
        self._built = True

    def even_rows(self) -> Iterator[Dict[str, str]]:
        """Yield rows at even indices (0, 2, 4, …)."""
        self._ensure_built()
        return iter(self._even)

    def odd_rows(self) -> Iterator[Dict[str, str]]:
        """Yield rows at odd indices (1, 3, 5, …)."""
        self._ensure_built()
        return iter(self._odd)

    def even_count(self) -> int:
        self._ensure_built()
        return len(self._even)

    def odd_count(self) -> int:
        self._ensure_built()
        return len(self._odd)

    def groups(self) -> Dict[str, List[Dict[str, str]]]:
        """Return both groups as a dict keyed by 'even' and 'odd'."""
        self._ensure_built()
        return {"even": list(self._even), "odd": list(self._odd)}
