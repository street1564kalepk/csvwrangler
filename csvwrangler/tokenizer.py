"""CSVTokenizer – splits a column's text into tokens and counts them."""
from __future__ import annotations
from typing import Iterable
import re


class CSVTokenizer:
    """Add a token-count column and/or a tokenized (space-joined) column.

    Parameters
    ----------
    source      : any object with .headers and .rows
    column      : column whose text will be tokenized
    count_col   : name for the new token-count column (None = skip)
    tokens_col  : name for the new tokens column (None = skip)
    pattern     : regex pattern used to split; default splits on non-word chars
    """

    def __init__(
        self,
        source,
        column: str,
        count_col: str | None = "token_count",
        tokens_col: str | None = None,
        pattern: str = r"\W+",
    ) -> None:
        if column not in source.headers:
            raise ValueError(f"Column '{column}' not found in headers.")
        if count_col is None and tokens_col is None:
            raise ValueError("At least one of count_col or tokens_col must be set.")
        existing = set(source.headers)
        for col in (count_col, tokens_col):
            if col is not None and col in existing:
                raise ValueError(
                    f"New column '{col}' conflicts with an existing header."
                )
        self._source = source
        self._column = column
        self._count_col = count_col
        self._tokens_col = tokens_col
        self._pattern = pattern
        self._extra = [c for c in (count_col, tokens_col) if c is not None]

    # ------------------------------------------------------------------
    @property
    def headers(self) -> list[str]:
        return list(self._source.headers) + self._extra

    def _tokenize(self, text: str) -> list[str]:
        return [t for t in re.split(self._pattern, text) if t]

    def rows(self) -> Iterable[dict]:
        for row in self._source.rows():
            tokens = self._tokenize(row.get(self._column, ""))
            new_row = dict(row)
            if self._count_col is not None:
                new_row[self._count_col] = len(tokens)
            if self._tokens_col is not None:
                new_row[self._tokens_col] = " ".join(tokens)
            yield new_row

    @property
    def row_count(self) -> int:
        return sum(1 for _ in self.rows())
