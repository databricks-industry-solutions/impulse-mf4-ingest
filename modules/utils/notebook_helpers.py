"""Notebook helpers for Databricks ingest jobs."""

import os
from typing import Any

import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from pyspark.sql.types import ArrayType, StringType, StructField, StructType

from .constants import DEFAULT_CATALOG, DEFAULT_SCHEMA
from .delta_ops import table_exists
from .mdf_comments import parse_md_comment_tags

_COMMENT_TAGS_SCHEMA = ArrayType(
    StructType([
        StructField("key", StringType()),
        StructField("value", StringType()),
    ])
)

def is_inf(column):
    """True for +Inf / -Inf (portable; F.isinf is not available on all Spark runtimes)."""
    # Build the literals per call so importing this module doesn't require a SparkSession.
    return (column == F.lit(float("inf"))) | (column == F.lit(float("-inf")))


def is_finite(column):
    """True when column is a non-null, non-NaN, non-infinite double/float."""
    return column.isNotNull() & ~F.isnan(column) & ~is_inf(column)


def finite_or_null(column):
    """Replace null, NaN, and ±Inf with SQL null."""
    return F.when(column.isNull() | F.isnan(column) | is_inf(column), None).otherwise(column)


def _notebook_dir(dbutils) -> str:
    return os.path.dirname(
        dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
    )


def notebook_dir(dbutils) -> str:
    """Return the workspace directory of the running notebook."""
    return _notebook_dir(dbutils)


def read_catalog_schema_widgets(dbutils, catalog_default: str = DEFAULT_CATALOG,
                                schema_default: str = DEFAULT_SCHEMA) -> tuple[str, str]:
    """Read catalog and schema job widgets."""
    dbutils.widgets.text("catalog", catalog_default)
    dbutils.widgets.text("schema", schema_default)
    return dbutils.widgets.get("catalog"), dbutils.widgets.get("schema")


def resolve_volume_path(catalog: str, schema: str, volume_or_path: str) -> str:
    """Return a UC Volume path from a volume name or absolute path."""
    path = volume_or_path.strip()
    if path.startswith("/Volumes/"):
        return path.rstrip("/")
    if path.startswith("dbfs:/Volumes/"):
        return path[len("dbfs:"):].rstrip("/")
    if path.startswith("/dbfs/Volumes/"):
        return path[len("/dbfs"):].rstrip("/")
    return f"/Volumes/{catalog}/{schema}/{path.strip('/')}"


def is_uc_volume_path(volume_or_path: str) -> bool:
    """True when the value is already an absolute UC Volume path."""
    path = volume_or_path.strip()
    return path.startswith(("/Volumes/", "dbfs:/Volumes/", "/dbfs/Volumes/"))


def require_status_table(spark, dbutils, catalog: str, schema: str) -> DataFrame:
    """Load the status table or exit the notebook when it is missing."""
    status_table_name = f"{catalog}.{schema}.status"
    if not table_exists(spark, catalog, schema, "status"):
        dbutils.notebook.exit("status table not found")
    return spark.read.table(status_table_name)


def get_batch_context(status_table_df: DataFrame, run_id: str) -> dict[str, Any]:
    """Collect batch file metadata for the current run_id."""
    rows = (
        status_table_df
        .where(F.col("run_id") == F.lit(run_id))
        .select("container_id", "filename")
        .collect()
    )
    open_files = [r["filename"] for r in rows]
    open_container_ids = [int(r["container_id"]) for r in rows]
    file2cid = {fn: cid for fn, cid in zip(open_files, open_container_ids)}
    return {
        "open_files": open_files,
        "open_container_ids": open_container_ids,
        "file2cid": file2cid,
        "open_files_csv": ",".join(open_files),
    }


def file2cid_map_expr(file2cid: dict[str, int]):
    """Build a Spark map expression from filename to container_id."""
    from itertools import chain
    return F.create_map([F.lit(x) for x in chain(*file2cid.items())])


def md_comment_tags_df(meta_df: DataFrame, group_cols: list[str]) -> DataFrame:
    """Expand md_comment into tag rows for the given grouping columns."""
    if len(group_cols) == 1:
        agg_expr = F.first("md_comment", ignorenulls=True).alias("md_comment")
        grouped = meta_df.groupBy(*group_cols).agg(agg_expr)
    else:
        grouped = meta_df.select(*group_cols, "md_comment")

    @F.udf(_COMMENT_TAGS_SCHEMA)
    def _md_comment_to_tags(md_comment):
        return [{"key": k, "value": v} for k, v in parse_md_comment_tags(md_comment)]

    return (
        grouped.withColumn("tag", F.explode(_md_comment_to_tags(F.col("md_comment"))))
        .select(
            *group_cols,
            F.col("tag.key").alias("key"),
            F.col("tag.value").alias("value"),
        )
    )


def filesize_mb_map(dbutils, filenames: list[str]) -> dict[str, float | None]:
    """Resolve file sizes on the driver for volume paths."""
    sizes: dict[str, float | None] = {}
    for path in filenames:
        if path is None:
            sizes[path] = None
            continue
        try:
            local_path = path
            if path.startswith("/Volumes/"):
                local_path = f"/dbfs{path}"
            sizes[path] = float(os.path.getsize(local_path)) / 1024 / 1024
        except OSError:
            sizes[path] = None
    return sizes
