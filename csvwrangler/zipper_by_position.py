"""CSVPositionZipper – merge two sources column-by-column (positional zip).

Unlike CSVJoiner (key-based) or CSVZipper (side-by-side header concat),
this module interleaves columns from two sources in a defined pattern:
  mode='interleave'  -> left_col1, right_col1, left_col2, right_col2, …
  mode='append'      -> all left cols then all right cols  (default)
  mode='alternate'   -> same as interleave (alias)

Rows are aligned by position; if sources differ in length the shorter one
pads with empty strings.
"""

from itertools import zip_longest
from typing import Iterator


class CSVPositionZipper:
    """Zip two CSV sources column-by-column."""

    _VALID_MODES = {"append", "interleave", "alternate"}

    def __init__(self, left, right, mode: str = "append"):
        if mode not in self._VALID_MODES:
            raise ValueError(
                f"mode must be one of {sorted(self._VALID_MODES)}, got {mode!r}"
            )
        self._left = left
        self._right = right
        self._mode = mode
        self._hdrs = self._build_headers()

    # ------------------------------------------------------------------
    def _build_headers(self) -> list:
        lh = list(self._left.headers)
        rh = list(self._right.headers)
        if self._mode == "append":
            return lh + rh
        # interleave / alternate
        result = []
        for pair in zip_longest(lh, rh):
            for h in pair:
                if h is not None:
                    result.append(h)
        return result

    @property
    def headers(self) -> list:
        return self._hdrs

    @property
    def row_count(self) -> int:
        lc = getattr(self._left, "row_count", None)
        rc = getattr(self._right, "row_count", None)
        if lc is not None and rc is not None:
            return max(lc, rc)
        return None

    def rows(self) -> Iterator[dict]:
        lh = list(self._left.headers)
        rh = list(self._right.headers)
        for lr, rr in zip_longest(
            self._left.rows(), self._right.rows(), fillvalue={}
        ):
            left_vals = [lr.get(h, "") for h in lh]
            right_vals = [rr.get(h, "") for h in rh]
            if self._mode == "append":
                values = left_vals + right_vals
                yield dict(zip(self._hdrs, values))
            else:
                values = []
                for pair in zip_longest(left_vals, right_vals, fillvalue=""):
                    values.extend(pair)
                yield dict(zip(self._hdrs, values))
