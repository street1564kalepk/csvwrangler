"""CSVInterpolator – fill missing numeric values via linear interpolation."""
from __future__ import annotations
from typing import Iterable, List


class CSVInterpolator:
    """Linearly interpolate missing (empty-string) values in numeric columns."""

    def __init__(self, source, columns: List[str]):
        self._source = source
        self._columns = columns
        self._validate()

    def _validate(self):
        missing = [c for c in self._columns if c not in self._source.headers]
        if missing:
            raise ValueError(f"Interpolator: unknown columns {missing}")

    @property
    def headers(self) -> List[str]:
        return self._source.headers

    def rows(self) -> Iterable[dict]:
        all_rows = list(self._source.rows())
        # Build per-column interpolated values
        for col in self._columns:
            values = [r[col] for r in all_rows]
            interpolated = _linear_interpolate(values)
            for row, val in zip(all_rows, interpolated):
                row[col] = val
        yield from all_rows

    @property
    def row_count(self) -> int:
        return sum(1 for _ in self.rows())


def _linear_interpolate(values: List[str]) -> List[str]:
    """Return list with empty strings replaced by linearly interpolated floats."""
    # Convert to float or None
    nums = []
    for v in values:
        try:
            nums.append(float(v))
        except (ValueError, TypeError):
            nums.append(None)

    n = len(nums)
    result = list(nums)

    i = 0
    while i < n:
        if result[i] is None:
            # find previous known
            left_i = i - 1
            # find next known
            right_i = i
            while right_i < n and result[right_i] is None:
                right_i += 1

            left_val = result[left_i] if left_i >= 0 else None
            right_val = result[right_i] if right_i < n else None

            for j in range(i, right_i):
                if left_val is None and right_val is None:
                    result[j] = 0.0
                elif left_val is None:
                    result[j] = right_val
                elif right_val is None:
                    result[j] = left_val
                else:
                    t = (j - left_i) / (right_i - left_i)
                    result[j] = left_val + t * (right_val - left_val)
            i = right_i
        else:
            i += 1

    # Convert back to strings, preserving int-like floats
    out = []
    for v in result:
        if v is None:
            out.append("")
        elif v == int(v):
            out.append(str(int(v)))
        else:
            out.append(str(round(v, 10)).rstrip('0').rstrip('.'))
    return out
