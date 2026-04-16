from typing import Iterator


class CSVCounter:
    """
    Counts occurrences of values in a given column and returns a summary table.
    Output headers: [column, count]
    """

    def __init__(self, source, column: str, sort_by: str = "count", ascending: bool = False):
        if sort_by not in ("count", "value"):
            raise ValueError(f"sort_by must be 'count' or 'value', got {sort_by!r}")
        if column not in source.headers:
            raise ValueError(f"Column {column!r} not found in source headers: {source.headers}")
        self._source = source
        self._column = column
        self._sort_by = sort_by
        self._ascending = ascending

    @property
    def headers(self) -> list[str]:
        return [self._column, "count"]

    def _build_counts(self) -> list[dict]:
        counts: dict[str, int] = {}
        for row in self._source.rows():
            val = row.get(self._column, "")
            counts[val] = counts.get(val, 0) + 1

        if self._sort_by == "count":
            key_fn = lambda item: item[1]
        else:
            key_fn = lambda item: item[0]

        sorted_items = sorted(counts.items(), key=key_fn, reverse=not self._ascending)
        return [{self._column: k, "count": str(v)} for k, v in sorted_items]

    def rows(self) -> Iterator[dict]:
        yield from self._build_counts()

    @property
    def row_count(self) -> int:
        return len(self._build_counts())

    def __repr__(self) -> str:
        return (
            f"CSVCounter(column={self._column!r}, sort_by={self._sort_by!r}, "
            f"ascending={self._ascending})"
        )
