"""Patch Pipeline with a .clip_by_percentile() method."""

from __future__ import annotations

from csvwrangler.clipper_by_percentile import CSVPercentileClipper


def clip_by_percentile(
    self,
    column: str,
    low_pct: float = 0.0,
    high_pct: float = 100.0,
):
    """Return a new Pipeline whose rows are filtered to those where *column*
    falls within the [*low_pct*, *high_pct*] percentile band.

    Parameters
    ----------
    column:
        Numeric column to evaluate.
    low_pct:
        Lower percentile bound (0–100).  Defaults to ``0``.
    high_pct:
        Upper percentile bound (0–100).  Defaults to ``100``.

    Example
    -------
    Keep only the top 25 % of rows by revenue::

        pipeline.clip_by_percentile("revenue", low_pct=75)
    """
    from csvwrangler.pipeline import Pipeline

    clipped = CSVPercentileClipper(
        self._source,
        column=column,
        low_pct=low_pct,
        high_pct=high_pct,
    )
    return Pipeline._from_source(clipped)


def _patch(pipeline_cls):
    pipeline_cls.clip_by_percentile = clip_by_percentile
