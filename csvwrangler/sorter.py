import operator
from typing import Iterable, Iterator, List, Optional


class CSVSorter:
    """
    Sorts rows from a CSV source by one or more columns.
    Supports ascending/descending and optional numeric casting.
    """

    def __init__(self, source, key: str, reverse: bool = False, numeric: bool = False):
        """
        :param source:   Any object with .headers and .rows (CSVReader, CSVFilter, etc.)
        :param key:      Column name to sort by.
        :param reverse:  If True, sort descending.
        :param numeric:  If True, cast values to float before comparing.
        """
        if key not in source.headers:
            raise ValueError(f"Sort key '{key}' not found in headers: {source.headers}")
        self._source = source
        self._key = key
        self._reverse = reverse
        self._numeric = numeric

    # ------------------------------------------------------------------
    # Public interface expected by Pipeline / CSVWriter
    # ------------------------------------------------------------------

    @property
    def headers(self) -> List[str]:
        return self._source.headers

    @property
    def rows(self) -> Iterator[dict]:
        """Materialise all rows, sort in memory, then yield."""
        key_fn = self._sort_key
        sorted_rows = sorted(self._source.rows, key=key_fn, reverse=self._reverse)
        return iter(sorted_rows)

    # ------------------------------------------------------------------
    # Chainable helpers
    # ------------------------------------------------------------------

    def then_sort(self, key: str, reverse: bool = False, numeric: bool = False) -> "CSVSorter":
        """Chain an additional sort pass (applied on top of this one)."""
        return CSVSorter(self, key=key, reverse=reverse, numeric=numeric)

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _sort_key(self, row: dict):
        value = row.get(self._key, "")
        if self._numeric:
            try:
                return float(value)
            except (ValueError, TypeError):
                return float("-inf")
        return value

    def __repr__(self) -> str:  # pragma: no cover
        direction = "desc" if self._reverse else "asc"
        return f"CSVSorter(key={self._key!r}, order={direction}, numeric={self._numeric})"
