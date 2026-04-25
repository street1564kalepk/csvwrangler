"""Patch Pipeline with a .smooth_columns() method."""
from __future__ import annotations

from typing import List, Optional


def smooth_columns(
    self,
    columns: List[str],
    window: int = 3,
    method: str = "mean",
    target_suffix: Optional[str] = None,
):
    """Smooth *columns* using a rolling *window* (``'mean'`` or ``'median'``).

    Parameters
    ----------
    columns:
        Column names whose values should be smoothed.
    window:
        Rolling-window size (default 3).
    method:
        ``'mean'`` (default) or ``'median'``.
    target_suffix:
        When provided, smoothed values are stored in ``<col><target_suffix>``
        instead of overwriting the source column.

    Returns
    -------
    Pipeline
        The same pipeline instance (chainable).
    """
    from csvwrangler.smoother import CSVSmoother

    self._source = CSVSmoother(
        self._source,
        columns=columns,
        window=window,
        method=method,
        target_suffix=target_suffix,
    )
    return self


def _patch(pipeline_cls):
    pipeline_cls.smooth_columns = smooth_columns
