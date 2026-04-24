"""CSVRegexSplitter – partition rows into groups based on a regex match
against a chosen column value.
"""
from __future__ import annotations

import re
from collections import defaultdict
from typing import Dict, Iterator, List


class CSVRegexSplitter:
    """Split a source into named groups where each group collects rows whose
    *column* value matches the corresponding regular expression pattern.

    Rows that match no pattern are collected under the key ``"__unmatched__"``.

    Parameters
    ----------
    source:
        Any object exposing ``.headers`` (list[str]) and ``.rows()``
        (iterator of dict[str, str]).
    column:
        The column whose value is tested against each pattern.
    patterns:
        Mapping of *group_name* -> *regex_string*.  Patterns are tested in
        insertion order; the first match wins.
    """

    UNMATCHED_KEY = "__unmatched__"

    def __init__(self, source, column: str, patterns: Dict[str, str]) -> None:
        if column not in source.headers:
            raise ValueError(f"Column '{column}' not found in headers: {source.headers}")
        self._source = source
        self._column = column
        self._patterns: Dict[str, re.Pattern] = {
            name: re.compile(pat) for name, pat in patterns.items()
        }
        self._built: Dict[str, List[dict]] | None = None

    # ------------------------------------------------------------------
    @property
    def headers(self) -> List[str]:
        return list(self._source.headers)

    # ------------------------------------------------------------------
    def _ensure_built(self) -> None:
        if self._built is not None:
            return
        buckets: Dict[str, List[dict]] = defaultdict(list)
        for row in self._source.rows():
            value = row.get(self._column, "")
            matched = False
            for name, pattern in self._patterns.items():
                if pattern.search(value):
                    buckets[name].append(row)
                    matched = True
                    break
            if not matched:
                buckets[self.UNMATCHED_KEY].append(row)
        self._built = dict(buckets)

    # ------------------------------------------------------------------
    @property
    def group_keys(self) -> List[str]:
        self._ensure_built()
        return list(self._built.keys())  # type: ignore[union-attr]

    def group_count(self) -> int:
        self._ensure_built()
        return len(self._built)  # type: ignore[arg-type]

    def rows(self, group: str) -> Iterator[dict]:
        """Yield rows belonging to *group*."""
        self._ensure_built()
        yield from self._built.get(group, [])  # type: ignore[union-attr]

    def row_count(self, group: str) -> int:
        self._ensure_built()
        return len(self._built.get(group, []))  # type: ignore[union-attr]
