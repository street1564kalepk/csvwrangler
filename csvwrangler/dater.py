"""CSVDater – parse and reformat date columns."""
from __future__ import annotations

from datetime import datetime
from typing import Iterable, List, Optional


class CSVDater:
    """Reformat one or more date columns from *in_fmt* to *out_fmt*.

    Parameters
    ----------
    source:
        Any object exposing ``.headers`` and ``.rows``.
    columns:
        Column names to reformat.
    in_fmt:
        ``strptime``-compatible input format string, e.g. ``"%Y-%m-%d"``.
    out_fmt:
        ``strftime``-compatible output format string, e.g. ``"%d/%m/%Y"``.
    errors:
        ``"raise"`` (default) – propagate ``ValueError`` on bad dates;
        ``"ignore"`` – leave the original value unchanged.
    """

    def __init__(
        self,
        source,
        columns: List[str],
        in_fmt: str,
        out_fmt: str,
        errors: str = "raise",
    ) -> None:
        if errors not in ("raise", "ignore"):
            raise ValueError("errors must be 'raise' or 'ignore'")
        self._source = source
        self._columns = list(columns)
        self._in_fmt = in_fmt
        self._out_fmt = out_fmt
        self._errors = errors

    # ------------------------------------------------------------------
    @property
    def headers(self) -> List[str]:
        return self._source.headers

    # ------------------------------------------------------------------
    def _reformat(self, value: str) -> str:
        try:
            return datetime.strptime(value, self._in_fmt).strftime(self._out_fmt)
        except (ValueError, TypeError):
            if self._errors == "raise":
                raise
            return value

    # ------------------------------------------------------------------
    def rows(self) -> Iterable[dict]:
        col_set = set(self._columns)
        for row in self._source.rows():
            yield {
                k: (self._reformat(v) if k in col_set else v)
                for k, v in row.items()
            }

    # ------------------------------------------------------------------
    @property
    def row_count(self) -> Optional[int]:
        return getattr(self._source, "row_count", None)
