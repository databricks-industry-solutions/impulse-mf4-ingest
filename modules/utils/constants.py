"""Shared constants for the MDF ingest accelerator."""

DEFAULT_CATALOG = "mda_demo"
DEFAULT_SCHEMA = "default"
DEFAULT_MDF4_VOLUME = "mdf4"
DEFAULT_CHECKPOINT_VOLUME = "mdf4_checkpoint"
DEFAULT_MAX_BATCH_SIZE = 100

STATUS_UNPROCESSED = "unprocessed"
STATUS_IN_PROGRESS = "in_progress"
STATUS_SUCCEEDED = "succeeded"
STATUS_FAILED = "failed"

DELTA_TARGET_FILE_SIZE_BYTES = 33_554_432  # 32 MiB

ROLLBACK_TABLES = (
    "channels",
    "channel_tags",
    "channel_metrics",
    "container_tags",
    "container_metrics",
    "bronze_meta",
    "masters",
)

OPTIMIZE_TABLES = (
    "container_tags",
    "container_metrics",
    "channel_tags",
    "channel_metrics",
    "channels",
    "bronze_meta",
    "masters",
)
