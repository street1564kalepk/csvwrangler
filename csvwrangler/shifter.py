"""CSVShifter – shift column values by a numeric offset or date delta."""
from __future__ import annotations
from typing import Iterable
import datetime


class CSVShifter:
    """Shift numeric or date column values by a fixed amount."""

    _SUPPORTED_DATE_FMTS = ["%Y-%m-%d", "%d/%m/%Y", "%m/%d/%Y"]

    def __init__(self, source, shifts: dict):
        """
        Parameters
        ----------
        source  : any object with .headers and .rows
        shifts  : {column: offset}  offset is int/float for numbers,
                  or a dict {"days": n} / {"months": n} for dates.
        """
        self._source = source
        self._shifts = shifts
        self._validate()

    def _validate(self):
        for col in self._shifts:
            if col not in self._source.headers:
                raise ValueError(f"Column '{col}' not found in headers.")

    @property
    def headers(self):
        return self._source.headers

    def _shift_value(self, value: str, offset):
        if isinstance(offset, dict):
            for fmt in self._SUPPORTED_DATE_FMTS:
                try:
                    dt = datetime.datetime.strptime(value, fmt)
                    dt += datetime.timedelta(**offset)
                    return dt.strftime(fmt)
                except ValueError:
                    continue
            return value  # unparseable – leave as-is
        try:
            num = float(value)
            result = num + offset
            return str(int(result)) if num == int(num) and isinstance(offset, int) else str(result)
        except (ValueError, TypeError):
            return value

    @property
    def rows(self) -> Iterable[dict]:
        for row in self._source.rows:
            new_row = dict(row)
            for col, offset in self._shifts.items():
                new_row[col] = self._shift_value(row.get(col, ""), offset)
            yield new_row

    @property
    def row_count(self) -> int:
        return sum(1 for _ in self.rows)
