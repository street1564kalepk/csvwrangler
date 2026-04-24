"""Auto-apply the highlighter patch when this module is imported."""
from csvwrangler.pipeline import Pipeline
from csvwrangler.pipeline_highlighter_patch import _patch

_patch(Pipeline)
