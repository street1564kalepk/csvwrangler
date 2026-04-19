"""CSVMasker – redact or mask column values."""
from __future__ import annotations
from typing import Iterable, Dict, Any


_STRATEGIES = {
    "redact": lambda v, ch, n: ch * len(v),
    "partial": lambda v, ch, n: v[:n] + ch * max(0, len(v) - n),
    "fixed": lambda v, ch, n: ch * 6,
}


class CSVMasker:
    """Mask values in specified columns.

    Parameters
    ----------
    source:
        Any object with ``headers`` and ``rows`` attributes.
    columns:
        Column names to mask.
    strategy:
        One of ``'redact'`` (default), ``'partial'``, or ``'fixed'``.
    char:
        Replacement character (default ``'*'``).
    visible:
        Number of leading characters to keep when strategy is ``'partial'``.
    """

    def __init__(
        self,
        source,
        columns: list[str],
        strategy: str = "redact",
        char: str = "*",
        visible: int = 2,
    ) -> None:
        if strategy not in _STRATEGIES:
            raise ValueError(
                f"Unknown strategy {strategy!r}. Choose from {list(_STRATEGIES)}"
            )
        unknown = [c for c in columns if c not in source.headers]
        if unknown:
            raise ValueError(f"Unknown columns: {unknown}")
        self._source = source
        self._columns = set(columns)
        self._fn = _STRATEGIES[strategy]
        self._char = char
        self._visible = visible

    @property
    def headers(self) -> list[str]:
        return self._source.headers

    def rows(self) -> Iterable[Dict[str, Any]]:
        for row in self._source.rows():
            yield {
                k: self._fn(str(v), self._char, self._visible)
                if k in self._columns
                else v
                for k, v in row.items()
            }

    @property
    def row_count(self) -> int:
        return sum(1 for _ in self.rows())
