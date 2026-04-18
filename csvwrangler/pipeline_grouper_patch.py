"""Patch Pipeline with a .group_by() method."""
from csvwrangler.grouper import CSVGrouper


def group_by(self, group_col: str, agg_col: str,
             agg_func: str = "count", out_col: str | None = None):
    """Group rows by *group_col* and aggregate *agg_col*.

    Returns a new Pipeline whose source is a CSVGrouper.
    """
    from csvwrangler.pipeline import Pipeline
    grouped = CSVGrouper(
        self._source, group_col, agg_col, agg_func, out_col
    )
    p = Pipeline.__new__(Pipeline)
    p._source = grouped
    return p


def _patch():
    from csvwrangler.pipeline import Pipeline
    Pipeline.group_by = group_by
