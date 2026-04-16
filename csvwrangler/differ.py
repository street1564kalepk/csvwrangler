"""CSVDiffer — compare two CSV sources and emit added/removed/changed rows."""
from __future__ import annotations
from typing import Iterator


class CSVDiffer:
    """Compare two CSV sources by a key column and tag differences."""

    VALID_MODES = ("added", "removed", "changed", "all")

    def __init__(self, left, right, key: str, mode: str = "all"):
        if mode not in self.VALID_MODES:
            raise ValueError(f"mode must be one of {self.VALID_MODES}")
        if key not in left.headers:
            raise ValueError(f"key '{key}' not in left headers")
        if key not in right.headers:
            raise ValueError(f"key '{key}' not in right headers")
        self._left = left
        self._right = right
        self._key = key
        self._mode = mode

    @property
    def headers(self) -> list[str]:
        combined = list(self._left.headers)
        for h in self._right.headers:
            if h not in combined:
                combined.append(h)
        return ["_diff"] + combined

    def rows(self) -> Iterator[dict]:
        key = self._key
        left_index = {row[key]: row for row in self._left.rows()}
        right_index = {row[key]: row for row in self._right.rows()}

        all_keys = list(left_index) + [k for k in right_index if k not in left_index]
        cols = self.headers[1:]

        for k in all_keys:
            in_left = k in left_index
            in_right = k in right_index

            if in_left and not in_right:
                tag = "removed"
                base = left_index[k]
            elif in_right and not in_left:
                tag = "added"
                base = right_index[k]
            else:
                lrow, rrow = left_index[k], right_index[k]
                tag = "changed" if any(lrow.get(c) != rrow.get(c) for c in cols if c != "_diff") else "unchanged"
                base = {c: rrow.get(c, lrow.get(c, "")) for c in cols}

            if self._mode != "all" and tag != self._mode:
                continue
            if tag == "unchanged" and self._mode == "all":
                continue

            row = {"_diff": tag}
            for c in cols:
                row[c] = base.get(c, "")
            yield row

    @property
    def row_count(self) -> int:
        return sum(1 for _ in self.rows())
