"""Patch Pipeline with a .clamp_columns() method."""

from __future__ import annotations

from csvwrangler.clamper import CSVClamper


def clamp_columns(
    self,
    columns: list[str],
    lower: float | None = None,
    upper: float | None = None,
):
    """Clamp *columns* to [lower, upper].  At least one bound is required.

    Returns self for chaining.
    """
    self._source = CSVClamper(
        self._source,
        columns=columns,
        lower=lower,
        upper=upper,
    )
    return self


def _patch(pipeline_cls):
    pipeline_cls.clamp_columns = clamp_columns
