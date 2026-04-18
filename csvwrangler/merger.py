"""CSVMerger – merge two sources by updating left rows with matching right rows."""
from __future__ import annotations
from typing import List, Dict, Any, Iterable


class CSVMerger:
    """Merge two CSV sources on a key column.

    For each row in *left*, if a matching row exists in *right* (by *on*),
    the left row's values are updated with the right row's values.
    Rows in left with no match are kept as-is.
    """

    def __init__(self, left, right, on: str, suffixes=("_left", "_right")):
        if on not in left.headers:
            raise ValueError(f"Key column '{on}' not found in left source")
        if on not in right.headers:
            raise ValueError(f"Key column '{on}' not found in right source")
        self._left = left
        self._right = right
        self._on = on
        self._suffixes = suffixes
        self._merged_headers: List[str] = self._build_headers()

    def _build_headers(self) -> List[str]:
        left_h = list(self._left.headers)
        right_extra = [
            c for c in self._right.headers if c != self._on
        ]
        result = list(left_h)
        for col in right_extra:
            if col in left_h:
                # rename conflicting columns
                result.append(col + self._suffixes[1])
            else:
                result.append(col)
        # rename original left conflicting columns
        final = []
        right_extra_set = set(right_extra)
        for col in result:
            base = col
            if base in right_extra_set and base in self._left.headers and base != self._on:
                final.append(base + self._suffixes[0])
            else:
                final.append(col)
        return final

    @property
    def headers(self) -> List[str]:
        return list(self._merged_headers)

    def _build_right_index(self) -> Dict[str, Dict[str, Any]]:
        index: Dict[str, Dict[str, Any]] = {}
        for row in self._right.rows():
            key = row[self._on]
            index[key] = row
        return index

    def rows(self) -> Iterable[Dict[str, Any]]:
        right_index = self._build_right_index()
        left_h = list(self._left.headers)
        right_extra = [c for c in self._right.headers if c != self._on]
        right_extra_set = set(right_extra)

        for left_row in self._left.rows():
            merged: Dict[str, Any] = {}
            for col in left_h:
                if col in right_extra_set and col != self._on:
                    merged[col + self._suffixes[0]] = left_row[col]
                else:
                    merged[col] = left_row[col]
            key = left_row[self._on]
            right_row = right_index.get(key, {})
            for col in right_extra:
                dest = (col + self._suffixes[1]) if col in left_h else col
                merged[dest] = right_row.get(col, "")
            yield merged

    @property
    def row_count(self) -> int:
        return sum(1 for _ in self.rows())
