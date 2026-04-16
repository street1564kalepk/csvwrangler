"""CSVNormalizer – normalise string values in specified columns.

Supported operations
--------------------
* ``strip``   – remove leading/trailing whitespace
* ``lower``   – convert to lower-case
* ``upper``   – convert to upper-case
* ``title``   – convert to title-case
* ``collapse``– collapse internal runs of whitespace to a single space
"""

import re
from typing import Dict, Iterable, Iterator, List


_OPS = {
    "strip": str.strip,
    "lower": str.lower,
    "upper": str.upper,
    "title": str.title,
    "collapse": lambda s: re.sub(r"\s+", " ", s).strip(),
}


class CSVNormalizer:
    """Apply one or more normalisation operations to selected columns."""

    def __init__(
        self,
        source,
        columns: List[str],
        operations: List[str],
    ) -> None:
        unknown_ops = [op for op in operations if op not in _OPS]
        if unknown_ops:
            raise ValueError(f"Unknown normalisation operations: {unknown_ops}")
        unknown_cols = [c for c in columns if c not in source.headers]
        if unknown_cols:
            raise ValueError(f"Columns not found in source: {unknown_cols}")

        self._source = source
        self._columns = set(columns)
        self._operations = [_OPS[op] for op in operations]

    @property
    def headers(self) -> List[str]:
        return self._source.headers

    def _normalise(self, value: str) -> str:
        for op in self._operations:
            value = op(value)
        return value

    def rows(self) -> Iterator[Dict[str, str]]:
        for row in self._source.rows():
            yield {
                k: (self._normalise(v) if k in self._columns else v)
                for k, v in row.items()
            }

    @property
    def row_count(self) -> int:
        return sum(1 for _ in self.rows())
