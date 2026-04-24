"""Patch Pipeline with a ``clip_by_value`` method."""

from __future__ import annotations

from csvwrangler.clipper_by_value import CSVValueClipper


def clip_by_value(
    self,
    columns: list[str],
    min_val: float | None = None,
    max_val: float | None = None,
):
    """Clamp numeric values in *columns* to the range [*min_val*, *max_val*].

    Either bound may be omitted (``None``) to apply only a one-sided clamp.

    Parameters
    ----------
    columns:
        Column names whose values should be clamped.
    min_val:
        Lower bound; values below this are set to *min_val*.
    max_val:
        Upper bound; values above this are set to *max_val*.

    Returns
    -------
    Pipeline
        The same pipeline instance (chainable).
    """
    self._source = CSVValueClipper(
        self._source,
        columns=columns,
        min_val=min_val,
        max_val=max_val,
    )
    return self


def _patch(pipeline_cls):
    pipeline_cls.clip_by_value = clip_by_value
