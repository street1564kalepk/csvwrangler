"""CSVExtractor – pull a substring or regex group from a column into a new column."""
from __future__ import annotations
import re
from typing import Iterator


class CSVExtractor:
    """Extract a pattern from *source_col* and write the result to *dest_col*.

    Parameters
    ----------
    source:      upstream source object (must expose .headers and .rows)
    source_col:  column to apply the pattern to
    pattern:     regular-expression string; first capture group (or full match)
                 is used as the extracted value
    dest_col:    name of the new column that receives the extracted value
    default:     value written when the pattern does not match (default "")
    """

    def __init__(
        self,
        source,
        source_col: str,
        pattern: str,
        dest_col: str,
        default: str = "",
    ) -> None:
        if source_col not in source.headers:
            raise ValueError(f"Column '{source_col}' not found in source headers")
        if dest_col in source.headers:
            raise ValueError(f"Column '{dest_col}' already exists in source headers")
        self._source = source
        self._source_col = source_col
        self._pattern = re.compile(pattern)
        self._dest_col = dest_col
        self._default = default

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    @property
    def headers(self) -> list[str]:
        return self._source.headers + [self._dest_col]

    def rows(self) -> Iterator[dict]:
        for row in self._source.rows():
            value = row.get(self._source_col, "")
            m = self._pattern.search(str(value))
            if m:
                extracted = m.group(1) if m.lastindex and m.lastindex >= 1 else m.group(0)
            else:
                extracted = self._default
            yield {**row, self._dest_col: extracted}

    @property
    def row_count(self) -> int:
        return sum(1 for _ in self.rows())
