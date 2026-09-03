# Databricks notebook source
# MAGIC %md
# MAGIC <img src="../../docs/flow_c.png" width="560" height="275">
# MAGIC
# MAGIC ### Ingest MDF4 batch via the `impulse_data_sources.mdf` Spark data sources
# MAGIC * Reads the current batch from the `status` table
# MAGIC * Writes RLE `mdf_signals` directly to `channels` (silver)
# MAGIC * Writes `mdf_metadata` to `bronze_meta`
# MAGIC * Optionally writes `mdf_masters` to `masters`

# COMMAND ----------

# MAGIC %md
# MAGIC #### Install the `databricks-impulse` MDF data sources
# MAGIC The MDF4 Spark data sources live in [`databrickslabs/impulse`](https://github.com/databrickslabs/impulse)
# MAGIC and are **not yet published to PyPI**, so we install directly from GitHub. This is
# MAGIC currently pinned to the `feature/mdfDataSources` branch.
# MAGIC
# MAGIC **To switch to the released package once the branch is merged**, change the ref on the
# MAGIC `%pip install` line below from `@feature/mdfDataSources` to `@main` (or pin a release
# MAGIC tag), or replace the whole spec with `databricks-impulse` once it is on PyPI.

# COMMAND ----------

# MAGIC %pip install --quiet "git+https://github.com/databrickslabs/impulse.git@feature/mdfDataSources"

# COMMAND ----------

# MAGIC %restart_python

# COMMAND ----------

# MAGIC %run ./bundle_bootstrap

# COMMAND ----------

import pyspark.sql.functions as F
from databricks.sdk import WorkspaceClient

import utils
from impulse_data_sources.mdf import register_mdf_datasources
from utils import schema_definitions
from utils.notebook_helpers import (
    file2cid_map_expr,
    get_batch_context,
    read_catalog_schema_widgets,
    require_status_table,
    resolve_volume_path,
)

# impulse registers the mdf_* data sources and tags a WorkspaceClient for read
# telemetry; WorkspaceClient() picks up the notebook's ambient auth.
register_mdf_datasources(spark, WorkspaceClient())


# COMMAND ----------

dbutils.widgets.text("mdf4_volume", "mdf4")
dbutils.widgets.dropdown("write_masters", "False", ["True", "False"])
dbutils.widgets.dropdown("partitioning", "group", ["group", "stripe"])
dbutils.widgets.text("target_partition_mb", "64")
dbutils.widgets.text("stripe_target_mb", "128")
dbutils.widgets.text("max_groups_per_partition", "64")

# COMMAND ----------

catalog, schema = read_catalog_schema_widgets(dbutils)
mdf4_volume = dbutils.widgets.get("mdf4_volume")
write_masters = dbutils.widgets.get("write_masters").lower() == "true"
partitioning = dbutils.widgets.get("partitioning")
target_partition_mb = dbutils.widgets.get("target_partition_mb")
stripe_target_mb = dbutils.widgets.get("stripe_target_mb")
max_groups_per_partition = dbutils.widgets.get("max_groups_per_partition")

channels_table = f"{catalog}.{schema}.channels"
bronze_meta_table = f"{catalog}.{schema}.bronze_meta"
masters_table = f"{catalog}.{schema}.masters"
mdf_root = resolve_volume_path(catalog, schema, mdf4_volume)

status_table_df = require_status_table(spark, dbutils, catalog, schema)

print("MDF path root:", mdf_root)
print("Writing channels to:", channels_table)
print("Writing metadata to:", bronze_meta_table)
print("write_masters:", write_masters)

# COMMAND ----------

current_run_id = dbutils.jobs.taskValues.get(taskKey="get_next_batch", key="next_run_id", debugValue="NA")
print("current_run_id:", current_run_id)

batch = get_batch_context(status_table_df, current_run_id)
open_files = batch["open_files"]
file2cid = batch["file2cid"]
open_files_csv = batch["open_files_csv"]

print("number of open files to convert:", len(open_files))
if not open_files:
  dbutils.notebook.exit("Nothing to do.")

file2cid_map = file2cid_map_expr(file2cid)

# COMMAND ----------

signals_df = (
    spark.read.format("mdf_signals")
    .option("path", mdf_root)
    .option("files", open_files_csv)
    .option("absolute_time", "true")
    .option("run_length_encoding", "true")
    .option("time_dtype", "float64")
    .option("value_dtype", "float32")
    .option("partitioning", partitioning)
    .option("target_partition_mb", target_partition_mb)
    .option("stripe_target_mb", stripe_target_mb)
    .option("max_groups_per_partition", max_groups_per_partition)
    .load()
)
channels_df = (
    signals_df
    .withColumn("container_id", file2cid_map[F.col("file_uri")].cast("bigint"))
    .select(
        "container_id",
        "channel_id",
        F.col("tstart").cast(schema_definitions.TIMESERIES_TIME_SQL_TYPE),
        F.col("tend").cast(schema_definitions.TIMESERIES_TIME_SQL_TYPE),
        F.col("value").cast(schema_definitions.TIMESERIES_VALUE_SQL_TYPE),
    )
)

(
    channels_df.write
    .clusterBy("container_id", "channel_id")
    .format("delta")
    .mode("append")
    .option("mergeSchema", "true")
    .saveAsTable(channels_table)
)

# COMMAND ----------

metadata_df = (
    spark.read.format("mdf_metadata")
    .option("path", mdf_root)
    .option("files", open_files_csv)
    .option("absolute_time", "true")
    .load()
)
meta_df = (
    metadata_df
    .withColumn("container_id", file2cid_map[F.col("file_uri")].cast("bigint"))
    .withColumnRenamed("file_uri", "filename")
    .select(*schema_definitions.BRONZE_META_OUTPUT_COLUMNS)
)

utils.merge_or_append_table(
    spark,
    meta_df,
    bronze_meta_table,
    "target.container_id = source.container_id AND target.channel_id = source.channel_id",
    ["container_id", "channel_id"],
)

# COMMAND ----------

if write_masters:
  masters_df = (
      spark.read.format("mdf_masters")
      .option("path", mdf_root)
      .option("files", open_files_csv)
      .option("absolute_time", "true")
      .option("time_dtype", "float64")
      .option("max_groups_per_partition", max_groups_per_partition)
      .load()
  )
  masters_out = (
      masters_df
      .withColumn("container_id", file2cid_map[F.col("file_uri")].cast("bigint"))
      .select(
          "container_id",
          "group_idx",
          F.col("timestamp").cast(schema_definitions.TIMESERIES_TIME_SQL_TYPE),
      )
  )
  (
      masters_out.write
      .clusterBy("container_id", "group_idx")
      .format("delta")
      .mode("append")
      .option("mergeSchema", "true")
      .saveAsTable(masters_table)
  )
