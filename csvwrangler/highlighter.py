"""CSVHighlighter – mark rows that match a condition with a flag column."""
from __future__ import annotations

from typing import Callable, Iterable


class CSVHighlighter:
    """Adds a boolean flag column whose value indicates whether each row
    satisfies *predicate*.

    Parameters
    ----------
    source:
        Any object exposing ``headers`` and ``rows``.
    predicate:
        A callable that receives a row dict and returns a truthy value.
    flag_column:
        Name of the new column to add (default ``"highlighted"``).
    true_value / false_value:
        String written when the predicate is / is not satisfied.
    """

    def __init__(
        self,
        source,
        predicate: Callable[[dict], bool],
        flag_column: str = "highlighted",
        true_value: str = "true",
        false_value: str = "false",
    ) -> None:
        if flag_column in source.headers:
            raise ValueError(
                f"Column '{flag_column}' already exists in source headers."
            )
        self._source = source
        self._predicate = predicate
        self._flag = flag_column
        self._true = true_value
        self._false = false_value

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    @property
    def headers(self) -> list[str]:
        return self._source.headers + [self._flag]

    def rows(self) -> Iterable[dict]:
        for row in self._source.rows():
            out = dict(row)
            out[self._flag] = self._true if self._predicate(row) else self._false
            yield out

    @property
    def row_count(self) -> int:
        return sum(1 for _ in self.rows())
