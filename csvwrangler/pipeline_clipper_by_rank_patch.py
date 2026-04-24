"""Patch Pipeline with a ``.clip_by_rank()`` method."""
from __future__ import annotations
from csvwrangler.clipper_by_rank import CSVRankClipper


def clip_by_rank(
    self,
    column: str,
    n: int,
    direction: str = "top",
):
    """Keep the top or bottom *n* rows ranked by *column*.

    Parameters
    ----------
    column:
        Numeric column to rank by.
    n:
        Number of rows to retain.
    direction:
        ``'top'`` (default) or ``'bottom'``.

    Returns
    -------
    Pipeline
        A new pipeline wrapping the clipped source.
    """
    from csvwrangler.pipeline import Pipeline

    new_src = CSVRankClipper(self._src, column=column, n=n, direction=direction)
    return Pipeline._from_src(new_src)


def _patch(pipeline_cls):
    pipeline_cls.clip_by_rank = clip_by_rank
