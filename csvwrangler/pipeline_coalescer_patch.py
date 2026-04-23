"""Patch Pipeline with a .coalesce() method."""
from typing import List


def coalesce(
    self,
    columns: List[str],
    target: str,
    empty_values=None,
):
    """Coalesce *columns* into *target*, using the first non-empty value.

    Parameters
    ----------
    columns     : ordered list of source column names (first wins)
    target      : destination column name
    empty_values: iterable of values treated as empty
                  (default: {"", None})
    """
    from csvwrangler.coalescer import CSVCoalescer
    from csvwrangler.pipeline import Pipeline

    kwargs = {"empty_values": empty_values} if empty_values is not None else {}
    new_source = CSVCoalescer(self._source, columns, target, **kwargs)
    p = Pipeline.__new__(Pipeline)
    p._source = new_source
    return p


def _patch(pipeline_cls):
    pipeline_cls.coalesce = coalesce
