"""CSVPadder – pad string values in specified columns to a fixed width."""

from __future__ import annotations
from typing import Iterable, Iterator


class CSVPadder:
    """Pad values in *columns* to *width* characters using *fillchar*.

    Parameters
    ----------
    source:    any object exposing ``.headers`` and ``.rows``
    columns:   column names to pad
    width:     target string width
    fillchar:  character used for padding (default ``' '``)
    align:     ``'left'``, ``'right'``, or ``'center'`` (default ``'left'``)
    """

    _ALIGNMENTS = {"left", "right", "center"}

    def __init__(
        self,
        source,
        columns: list[str],
        width: int,
        fillchar: str = " ",
        align: str = "left",
    ) -> None:
        if align not in self._ALIGNMENTS:
            raise ValueError(f"align must be one of {self._ALIGNMENTS}")
        if len(fillchar) != 1:
            raise ValueError("fillchar must be a single character")
        if width < 1:
            raise ValueError("width must be >= 1")
        unknown = [c for c in columns if c not in source.headers]
        if unknown:
            raise KeyError(f"Unknown columns: {unknown}")
        self._source = source
        self._columns = set(columns)
        self._width = width
        self._fillchar = fillchar
        self._align = align

    @property
    def headers(self) -> list[str]:
        return list(self._source.headers)

    def _pad(self, value: str) -> str:
        """Pad *value* to ``self._width`` using the configured alignment.

        Values that are already at or beyond *width* are returned unchanged.
        """
        if self._align == "left":
            return value.ljust(self._width, self._fillchar)
        if self._align == "right":
            return value.rjust(self._width, self._fillchar)
        return value.center(self._width, self._fillchar)

    def rows(self) -> Iterator[dict]:
        for row in self._source.rows():
            yield {
                k: self._pad(str(v)) if k in self._columns else v
                for k, v in row.items()
            }

    @property
    def row_count(self) -> int:
        return sum(1 for _ in self.rows())
