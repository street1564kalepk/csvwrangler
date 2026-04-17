"""CSVCondenser – collapse consecutive rows by a key column using an aggregation."""
from typing import Iterator

_SUPPORTED = {"first", "last", "sum", "count", "join"}


class CSVCondenser:
    """Collapse runs of rows that share the same key value."""

    def __init__(self, source, key: str, agg: str = "first", sep: str = "|" ):
        if agg not in _SUPPORTED:
            raise ValueError(f"agg must be one of {_SUPPORTED}, got {agg!r}")
        if key not in source.headers:
            raise ValueError(f"Key column {key!r} not found in headers")
        self._source = source
        self._key = key
        self._agg = agg
        self._sep = sep

    @property
    def headers(self):
        return self._source.headers

    def _merge(self, group: list[dict]) -> dict:
        if not group:
            return {}
        first = group[0]
        if self._agg == "first":
            return dict(first)
        if self._agg == "last":
            return dict(group[-1])
        result = {self._key: first[self._key]}
        for col in self.headers:
            if col == self._key:
                continue
            vals = [r[col] for r in group]
            if self._agg == "count":
                result[col] = str(len(vals))
            elif self._agg == "join":
                result[col] = self._sep.join(vals)
            elif self._agg == "sum":
                try:
                    result[col] = str(sum(float(v) for v in vals if v != ""))
                except ValueError:
                    result[col] = self._sep.join(vals)
            else:
                result[col] = vals[0]
        return result

    def rows(self) -> Iterator[dict]:
        group: list[dict] = []
        current_key = object()
        for row in self._source.rows():
            k = row[self._key]
            if k != current_key:
                if group:
                    yield self._merge(group)
                group = [row]
                current_key = k
            else:
                group.append(row)
        if group:
            yield self._merge(group)

    @property
    def row_count(self) -> int:
        return sum(1 for _ in self.rows())
