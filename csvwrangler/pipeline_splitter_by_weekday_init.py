"""Auto-apply the split_by_weekday patch when this module is imported."""
from csvwrangler.pipeline import Pipeline
from csvwrangler.pipeline_splitter_by_weekday_patch import _patch

_patch(Pipeline)
