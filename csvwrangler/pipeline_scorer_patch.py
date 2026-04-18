"""Patch Pipeline with a .score() method."""
from __future__ import annotations
from csvwrangler.scorer import CSVScorer


def score(self, weights: dict, score_col: str = "score", default: float = 0.0):
    """Append a weighted-sum score column to the pipeline."""
    return self._wrap(CSVScorer(self._source, weights, score_col=score_col, default=default))


def _patch(pipeline_cls):
    pipeline_cls.score = score
