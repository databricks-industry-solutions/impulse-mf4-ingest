"""Delta Lake helpers for status updates and table operations."""

from functools import reduce
from typing import List

import pyspark.sql.functions as F
from delta.tables import DeltaTable
from pyspark.sql import DataFrame

from .constants import OPTIMIZE_TABLES, ROLLBACK_TABLES, STATUS_SUCCEEDED


def table_exists(spark, catalog: str, schema: str, table_name: str) -> bool:
    """Check whether a Unity Catalog table exists."""
    safe_catalog = catalog.replace("'", "''")
    safe_schema = schema.replace("'", "''")
    safe_table = table_name.replace("'", "''")
    query = spark.sql(f"""
            SELECT 1
            FROM {safe_catalog}.information_schema.tables
            WHERE table_name = '{safe_table}'
            AND table_schema = '{safe_schema}' LIMIT 1""")
    return query.count() > 0


def upsert_set(
    spark,
    table_name: str,
    source_df: DataFrame,
    merge_cond: List[str],
    update_cols: List[str],
) -> None:
    """Perform an upsert (merge) into a Delta table from a source DataFrame."""
    target_df = DeltaTable.forName(spark, table_name)
    merge_key_exprs = map(lambda x: f"s.{x} = t.{x}", merge_cond)
    merge_cond_str = reduce(lambda x, y: f"{x} AND {y}", merge_key_exprs)
    update_set = {c: f"s.{c}" for c in update_cols}
    (
        target_df.alias("t")
        .merge(source_df.alias("s"), F.expr(merge_cond_str))
        .whenMatchedUpdate(set=update_set)
        .whenNotMatchedInsertAll()
        .execute()
    )


def update_status(spark, status_table_name: str, run_id: str, new_status: str) -> None:
    """Update the status column for rows with a specific run_id."""
    updates_df = (
        spark.read.table(status_table_name)
        .where(F.col("run_id") == F.lit(run_id))
        .withColumn("status", F.lit(new_status))
    )
    if new_status == STATUS_SUCCEEDED:
        updates_df = updates_df.withColumn("_processing_done_ts", F.now())
        upsert_set(
            spark,
            status_table_name,
            updates_df,
            ["filename", "run_id"],
            ["status", "_processing_done_ts"],
        )
    else:
        upsert_set(spark, status_table_name, updates_df, ["filename", "run_id"], ["status"])


def get_files_by_status(status_table_df: DataFrame, status: str) -> DataFrame:
    """Filter a status table DataFrame by a specific status value."""
    return status_table_df.where(F.col("status") == F.lit(status))


def merge_or_append_table(
    spark,
    df: DataFrame,
    table_name: str,
    merge_condition: str,
    cluster_by: list[str],
    merge_schema: bool = True,
) -> None:
    """Create a Delta table on first write; merge on subsequent writes."""
    if not spark.catalog.tableExists(table_name):
        writer = (
            df.write.clusterBy(*cluster_by)
            .format("delta")
            .mode("append")
        )
        if merge_schema:
            writer = writer.option("mergeSchema", "true")
        writer.saveAsTable(table_name)
        return

    (
        DeltaTable.forName(spark, table_name)
        .alias("target")
        .merge(df.alias("source"), merge_condition)
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )


def rollback_batch_tables(spark, catalog: str, schema: str, container_ids: list) -> None:
    """Delete batch rows from all ingest output tables."""
    if not container_ids:
        return
    for table_name in ROLLBACK_TABLES:
        full_name = f"{catalog}.{schema}.{table_name}"
        if not spark.catalog.tableExists(full_name):
            continue
        DeltaTable.forName(spark, full_name).delete(F.col("container_id").isin(container_ids))


def optimize_tables(
    spark,
    catalog: str,
    schema: str,
    table_names: tuple[str, ...] = OPTIMIZE_TABLES,
) -> None:
    """Run OPTIMIZE compaction on tables that exist."""
    for table_name in table_names:
        full_name = f"{catalog}.{schema}.{table_name}"
        if spark.catalog.tableExists(full_name):
            DeltaTable.forName(spark, full_name).optimize().executeCompaction()
