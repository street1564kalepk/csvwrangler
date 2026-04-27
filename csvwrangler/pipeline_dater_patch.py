"""Patch Pipeline with a ``.reformat_dates()`` method backed by CSVDater."""
from __future__ import annotations

from typing import List


def reformat_dates(
    self,
    columns: List[str],
    in_fmt: str,
    out_fmt: str,
    errors: str = "raise",
):
    """Return a new Pipeline whose *columns* have dates reformatted.

    Parameters
    ----------
    columns:
        Column names to reformat.
    in_fmt:
        Input ``strptime`` format, e.g. ``"%Y-%m-%d"``.
    out_fmt:
        Output ``strftime`` format, e.g. ``"%d/%m/%Y"``.
    errors:
        ``"raise"`` or ``"ignore"``.
    """
    from csvwrangler.dater import CSVDater
    from csvwrangler.pipeline import Pipeline

    dater = CSVDater(self._source, columns, in_fmt, out_fmt, errors=errors)
    return Pipeline._from_source(dater)


def _patch(pipeline_cls):
    pipeline_cls.reformat_dates = reformat_dates
