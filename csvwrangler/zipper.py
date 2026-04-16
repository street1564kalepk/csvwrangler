"""CSVZipper: merge two sources column-wise (zip join)."""

from typing import Iterator


class CSVZipper:
    """Combine two CSV sources side-by-side by row position.

    Rows are emitted until the shorter source is exhausted.
    Duplicate column names from the right source are suffixed with '_right'.
    """

    def __init__(self, left, right):
        self._left = left
        self._right = right
        self._headers = self._build_headers()

    def _build_headers(self):
        left_h = list(self._left.headers)
        right_h = []
        left_set = set(left_h)
        for h in self._right.headers:
            if h in left_set:
                right_h.append(h + "_right")
            else:
                right_h.append(h)
        return left_h + right_h

    @property
    def headers(self):
        return self._headers

    def rows(self) -> Iterator[dict]:
        right_raw = list(self._right.headers)
        right_renamed = self._headers[len(self._left.headers):]
        rename_map = dict(zip(right_raw, right_renamed))

        for left_row, right_row in zip(self._left.rows(), self._right.rows()):
            merged = dict(left_row)
            for orig, renamed in rename_map.items():
                merged[renamed] = right_row.get(orig, "")
            yield merged

    def row_count(self) -> int:
        return sum(1 for _ in self.rows())
