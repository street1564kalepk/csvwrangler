"""CSVLengthSplitter – split rows into groups based on string length of a column value."""

from __future__ import annotations
from typing import Dict, Iterator, List


class CSVLengthSplitter:
    """Split source rows into buckets keyed by the character-length of a column value.

    Rows whose column value has length < short_threshold go into the "short" bucket,
    those with length >= long_threshold go into "long", and everything in between
    goes into "medium".
    """

    def __init__(
        self,
        source,
        column: str,
        short_threshold: int = 5,
        long_threshold: int = 10,
    ) -> None:
        if short_threshold >= long_threshold:
            raise ValueError(
                f"short_threshold ({short_threshold}) must be less than "
                f"long_threshold ({long_threshold})"
            )
        self._source = source
        self._column = column
        self._short = short_threshold
        self._long = long_threshold
        self._groups: Dict[str, List[dict]] | None = None

    @property
    def headers(self) -> List[str]:
        return list(self._source.headers)

    def _ensure_built(self) -> None:
        if self._groups is not None:
            return
        self._groups = {"short": [], "medium": [], "long": []}
        col = self._column
        short = self._short
        long_ = self._long
        for row in self._source.rows:
            val = str(row.get(col, ""))
            length = len(val)
            if length < short:
                self._groups["short"].append(row)
            elif length >= long_:
                self._groups["long"].append(row)
            else:
                self._groups["medium"].append(row)

    @property
    def group_keys(self) -> List[str]:
        self._ensure_built()
        return [k for k, v in self._groups.items() if v]

    def rows_for(self, key: str) -> Iterator[dict]:
        self._ensure_built()
        if key not in self._groups:
            raise KeyError(f"Unknown length group: {key!r}")
        yield from self._groups[key]

    def row_count_for(self, key: str) -> int:
        self._ensure_built()
        return len(self._groups.get(key, []))
