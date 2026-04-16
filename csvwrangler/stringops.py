"""CSVStringOps — apply string operations to columns."""

from typing import Iterable

_SUPPORTED = {"upper", "lower", "strip", "title", "lstrip", "rstrip"}


class CSVStringOps:
    """Apply string operations to one or more columns."""

    def __init__(self, source, ops: dict):
        """
        Parameters
        ----------
        source : any object with .headers and .rows()
        ops    : {column_name: operation_name, ...}
                 e.g. {"name": "upper", "city": "strip"}
        """
        unknown_ops = set(ops.values()) - _SUPPORTED
        if unknown_ops:
            raise ValueError(
                f"Unsupported string operation(s): {unknown_ops}. "
                f"Supported: {_SUPPORTED}"
            )
        missing = set(ops.keys()) - set(source.headers)
        if missing:
            raise ValueError(f"Column(s) not found in source: {missing}")

        self._source = source
        self._ops = ops

    @property
    def headers(self):
        return self._source.headers

    def rows(self) -> Iterable[dict]:
        for row in self._source.rows():
            new_row = dict(row)
            for col, op in self._ops.items():
                val = new_row.get(col, "")
                if isinstance(val, str):
                    new_row[col] = getattr(val, op)()
            yield new_row

    def row_count(self) -> int:
        return sum(1 for _ in self.rows())

    def __repr__(self):
        return f"CSVStringOps(ops={self._ops})"
