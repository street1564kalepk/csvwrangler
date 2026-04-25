"""Split CSV rows into groups based on the weekday of a date column."""
from __future__ import annotations

import datetime
from typing import Dict, Iterator, List

_WEEKDAY_NAMES = [
    "Monday", "Tuesday", "Wednesday", "Thursday",
    "Friday", "Saturday", "Sunday",
]

_FORMATS = ["%Y-%m-%d", "%d/%m/%Y", "%m/%d/%Y", "%Y/%m/%d"]


def _parse_date(value: str) -> datetime.date:
    for fmt in _FORMATS:
        try:
            return datetime.datetime.strptime(value.strip(), fmt).date()
        except ValueError:
            continue
    raise ValueError(f"Cannot parse date: {value!r}")


class CSVWeekdaySplitter:
    """Split rows by the weekday of *column*.

    Groups are keyed by weekday name ("Monday" … "Sunday").
    Rows whose date cannot be parsed are placed in the "_unparsed" bucket.
    """

    def __init__(self, source, column: str) -> None:
        self._source = source
        self._column = column
        self._groups: Dict[str, List[dict]] = {}
        self._built = False

    @property
    def headers(self) -> List[str]:
        return list(self._source.headers)

    def _ensure_built(self) -> None:
        if self._built:
            return
        self._groups = {name: [] for name in _WEEKDAY_NAMES}
        self._groups["_unparsed"] = []
        for row in self._source.rows():
            raw = row.get(self._column, "")
            try:
                wd = _parse_date(raw).weekday()
                self._groups[_WEEKDAY_NAMES[wd]].append(row)
            except ValueError:
                self._groups["_unparsed"].append(row)
        self._built = True

    @property
    def group_keys(self) -> List[str]:
        self._ensure_built()
        return [k for k, v in self._groups.items() if v]

    def group(self, key: str) -> List[dict]:
        self._ensure_built()
        return list(self._groups.get(key, []))

    def rows(self, key: str) -> Iterator[dict]:
        for row in self.group(key):
            yield row

    @property
    def group_count(self) -> int:
        return len(self.group_keys)
