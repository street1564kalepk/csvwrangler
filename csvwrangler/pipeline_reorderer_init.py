"""Auto-apply reorderer patch when this module is imported."""
from csvwrangler.pipeline import Pipeline
from csvwrangler.pipeline_reorderer_patch import _patch

_patch(Pipeline)
