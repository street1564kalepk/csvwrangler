"""Patch Pipeline with reorder_columns method."""
from __future__ import annotations
from csvwrangler.reorderer import CSVReorderer


def reorder_columns(self, order: list[str], drop_rest: bool = False):
    """Reorder (and optionally drop) columns.

    Args:
        order: Column names in desired order.
        drop_rest: If True, columns not in *order* are dropped.

    Returns:
        Pipeline: self, for chaining.
    """
    self._source = CSVReorderer(self._source, order, drop_rest=drop_rest)
    return self


def _patch(pipeline_cls):
    pipeline_cls.reorder_columns = reorder_columns
