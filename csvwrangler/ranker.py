"""CSVRanker – adds a rank column based on a sort key."""
from __future__ import annotations
from typing import Iterable, Iterator


class CSVRanker:
    """Adds a rank column to rows ordered by *sort_col*.

    Parameters
    ----------
    source      : any object exposing .headers and .rows
    sort_col    : column name to rank by
    rank_col    : name of the new rank column (default ``"rank"``)
    ascending   : sort order (default ``True``)
    dense       : if ``True`` use dense ranking (no gaps); default ``False``
    """

    def __init__(
        self,
        source,
        sort_col: str,
        rank_col: str = "rank",
        ascending: bool = True,
        dense: bool = False,
    ) -> None:
        if sort_col not in source.headers:
            raise ValueError(f"Column '{sort_col}' not found in source headers.")
        if rank_col in source.headers:
            raise ValueError(f"Rank column '{rank_col}' already exists in headers.")
        self._source = source
        self._sort_col = sort_col
        self._rank_col = rank_col
        self._ascending = ascending
        self._dense = dense

    @property
    def headers(self) -> list[str]:
        return self._source.headers + [self._rank_col]

    def rows(self) -> Iterator[dict]:
        all_rows = list(self._source.rows())
        try:
            keyed = [(float(r[self._sort_col]), r) for r in all_rows]
        except (ValueError, TypeError):
            keyed = [(r[self._sort_col], r) for r in all_rows]

        sorted_rows = sorted(keyed, key=lambda x: x[0], reverse=not self._ascending)

        # Build value -> rank mapping
        rank_map: dict = {}
        current_rank = 1
        for i, (val, _) in enumerate(sorted_rows):
            if val not in rank_map:
                rank_map[val] = current_rank
                if self._dense:
                    current_rank += 1
                else:
                    current_rank = i + 2  # next rank after gap

        # Re-emit in original order
        for row in all_rows:
            try:
                key = float(row[self._sort_col])
            except (ValueError, TypeError):
                key = row[self._sort_col]
            yield {**row, self._rank_col: rank_map[key]}

    @property
    def row_count(self) -> int:
        return sum(1 for _ in self.rows())
