"""Auto-import shim so that importing csvwrangler activates the partition patch."""
import csvwrangler.pipeline_partitioner_patch  # noqa: F401
