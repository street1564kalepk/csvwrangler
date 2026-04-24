"""CSVNthSplitter – splits rows into two groups: every Nth row vs. the rest."""

from typing import List, Dict


class CSVNthSplitter:
    """Partition a CSV source into two groups based on row index modulo *n*.

    Every row whose 1-based index is divisible by *n* lands in the
    ``nth`` group; all other rows land in the ``rest`` group.

    Parameters
    ----------
    source:
        Any object exposing ``.headers`` and ``.rows()``.
    n:
        The stride.  Must be a positive integer >= 2.
    """

    def __init__(self, source, n: int) -> None:
        if not isinstance(n, int) or n < 2:
            raise ValueError("n must be an integer >= 2")
        self._source = source
        self._n = n
        self._nth: List[Dict] = []
        self._rest: List[Dict] = []
        self._built = False

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    @property
    def headers(self) -> List[str]:
        return self._source.headers

    def _ensure_built(self) -> None:
        if self._built:
            return
        for idx, row in enumerate(self._source.rows(), start=1):
            if idx % self._n == 0:
                self._nth.append(row)
            else:
                self._rest.append(row)
        self._built = True

    def nth_rows(self):
        """Yield every Nth row (1-based index divisible by *n*)."""
        self._ensure_built()
        yield from self._nth

    def rest_rows(self):
        """Yield all rows *not* in the nth group."""
        self._ensure_built()
        yield from self._rest

    @property
    def nth_count(self) -> int:
        self._ensure_built()
        return len(self._nth)

    @property
    def rest_count(self) -> int:
        self._ensure_built()
        return len(self._rest)

    @property
    def row_count(self) -> int:
        return self.nth_count + self.rest_count
