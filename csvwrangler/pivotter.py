"""CSVPivotter: pivot rows into columns by a key column and value column."""
from typing import Iterator


class CSVPivotter:
    """
    Pivot a CSV source so that unique values in *pivot_col* become new column
    headers, filled with corresponding values from *value_col*.

    Rows are grouped by *index_col*.  If multiple rows share the same
    (index, pivot) pair the *last* value wins.

    Example
    -------
    Input:
        name, metric, value
        alice, height, 170
        alice, weight, 65
        bob,   height, 180
        bob,   weight, 80

    pivot(index_col='name', pivot_col='metric', value_col='value') ->
        name, height, weight
        alice, 170, 65
        bob, 180, 80
    """

    def __init__(self, source, index_col: str, pivot_col: str, value_col: str):
        self._source = source
        self._index_col = index_col
        self._pivot_col = pivot_col
        self._value_col = value_col
        self._pivot_values: list[str] | None = None
        self._data: dict | None = None

    def _build(self):
        if self._data is not None:
            return
        pivot_values_seen: dict[str, int] = {}  # preserves insertion order
        data: dict[str, dict[str, str]] = {}
        for row in self._source.rows():
            idx = row[self._index_col]
            pv = row[self._pivot_col]
            val = row[self._value_col]
            pivot_values_seen.setdefault(pv, 1)
            data.setdefault(idx, {})[pv] = val
        self._pivot_values = list(pivot_values_seen.keys())
        self._data = data

    @property
    def headers(self) -> list[str]:
        self._build()
        return [self._index_col] + self._pivot_values  # type: ignore[operator]

    def rows(self) -> Iterator[dict[str, str]]:
        self._build()
        for idx, mapping in self._data.items():  # type: ignore[union-attr]
            row = {self._index_col: idx}
            for pv in self._pivot_values:  # type: ignore[union-attr]
                row[pv] = mapping.get(pv, "")
            yield row

    @property
    def row_count(self) -> int:
        self._build()
        return len(self._data)  # type: ignore[arg-type]
