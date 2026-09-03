# Databricks notebook source
# MAGIC %md
# MAGIC <img src="../../docs/flow_d.png" width="560" height="275">
# MAGIC
# MAGIC ### Derive and persist container-level tags and metrics from `channels` / `bronze_meta`.
# MAGIC * Reads context (catalog/schema), identifies the current batch, and exits if none
# MAGIC * Builds file tags (path, size) and `md_comment`-derived tags from `bronze_meta`
# MAGIC * Aggregates metrics (channel count, time bounds) from `channels` and writes to `container_metrics`

# COMMAND ----------

# MAGIC %run ./bundle_bootstrap

# COMMAND ----------

import pyspark.sql.functions as F

import utils
from utils import schema_definitions
from utils.notebook_helpers import (
    filesize_mb_map,
    finite_or_null,
    get_batch_context,
    is_finite,
    md_comment_tags_df,
    read_catalog_schema_widgets,
    require_status_table,
)

# COMMAND ----------

catalog, schema = read_catalog_schema_widgets(dbutils)
status_table_df = require_status_table(spark, dbutils, catalog, schema)

container_tags_table = f"{catalog}.{schema}.container_tags"
container_metrics_table = f"{catalog}.{schema}.container_metrics"
channels_table = f"{catalog}.{schema}.channels"
bronze_meta_table = f"{catalog}.{schema}.bronze_meta"

print("container_tags_table", container_tags_table)
print("container_metrics_table", container_metrics_table)

# COMMAND ----------

current_run_id = dbutils.jobs.taskValues.get(taskKey="get_next_batch", key="next_run_id", debugValue="NA")
batch = get_batch_context(status_table_df, current_run_id)
open_files = batch["open_files"]
open_container_ids = batch["open_container_ids"]

print("number of open files to convert:", len(open_files))
if not open_files:
  dbutils.notebook.exit("Nothing to do.")

# COMMAND ----------

batch_files_df = spark.createDataFrame(
    [(int(cid), fn) for cid, fn in zip(open_container_ids, open_files)],
    "container_id long, filename string",
)

channels_df = (
    spark.read.table(channels_table)
    .where(F.col("container_id").isin(open_container_ids))
)

meta_df = (
    spark.read.table(bronze_meta_table)
    .where(F.col("container_id").isin(open_container_ids))
)

header_dt_df = (
    meta_df.groupBy("container_id")
    .agg(F.min("header_datetime").alias("header_datetime"))
)

_time = schema_definitions.TIMESERIES_TIME_SQL_TYPE

channels_valid = channels_df.filter(
    is_finite(F.col("tstart")) & is_finite(F.col("tend"))
)

container_meta_df = (
    channels_valid
    .groupBy("container_id")
    .agg(
        F.countDistinct("channel_id").cast("int").alias("num_channels"),
        F.min("tstart").cast(_time).alias("start_ts"),
        F.max("tend").cast(_time).alias("stop_ts"),
    )
    .join(batch_files_df, on="container_id", how="left")
    .join(header_dt_df, on="container_id", how="left")
    .withColumn(
        "duration_s",
        finite_or_null(
            F.when(
                is_finite(F.col("start_ts")) & is_finite(F.col("stop_ts")),
                (F.col("stop_ts") - F.col("start_ts")).cast(_time),
            )
        ),
    )
    .withColumn("start_dt", F.col("header_datetime"))
    .withColumn(
        "stop_dt",
        F.when(
            F.col("header_datetime").isNotNull()
            & is_finite(F.col("duration_s")),
            F.from_unixtime(
                finite_or_null(
                    F.unix_timestamp(F.col("header_datetime")) + F.col("duration_s")
                )
            ),
        ),
    )
)

# COMMAND ----------

file_sizes = filesize_mb_map(dbutils, open_files)
file_sizes_df = spark.createDataFrame(
    [(int(cid), fn, file_sizes.get(fn)) for cid, fn in zip(open_container_ids, open_files)],
    "container_id long, filename string, filesize_mb double",
)

FILE_TAG_COLS = ["file_uri", "base_uri", "filename", "filesize_mb"]

file_tags_df = (
    container_meta_df.select(
        "container_id",
        F.col("filename").alias("file_uri"),
        F.regexp_replace(F.col("filename"), r"/[^/]*$", F.lit("")).alias("base_uri"),
        F.element_at(F.split(F.col("filename"), "/"), -1).alias("filename"),
    )
    .join(file_sizes_df.select("container_id", F.col("filesize_mb").cast("string")), on="container_id")
    .unpivot(["container_id"], FILE_TAG_COLS, "key", "value")
    .withColumn("value", F.col("value").cast("string"))
)

comment_tags_df = md_comment_tags_df(meta_df, ["container_id"])
tagsdf = file_tags_df.unionByName(comment_tags_df)

# COMMAND ----------

utils.merge_or_append_table(
    spark,
    tagsdf,
    container_tags_table,
    "target.container_id = source.container_id and target.key = source.key",
    ["container_id"],
)

# COMMAND ----------

vehicle_key_df = (
    comment_tags_df
    .where(F.col("key") == F.lit("vehicle_key"))
    .select("container_id", F.col("value").alias("vehicle_key"))
)

metricsdf = (
    container_meta_df
    .join(vehicle_key_df, on="container_id", how="left")
    .withColumn("vehicle_key", F.coalesce(F.col("vehicle_key"), F.lit("unknown")))
    .select(
        F.col("container_id").cast("long"),
        F.col("vehicle_key").cast("string"),
        finite_or_null(F.col("start_ts")).cast(_time).alias("start_ts"),
        finite_or_null(F.col("stop_ts")).cast(_time).alias("stop_ts"),
        F.col("start_dt").cast("timestamp"),
        F.col("stop_dt").cast("timestamp"),
        finite_or_null(F.col("duration_s")).cast(_time).alias("duration_s"),
        F.col("num_channels").cast("int"),
    )
)

# COMMAND ----------

utils.merge_or_append_table(
    spark,
    metricsdf,
    container_metrics_table,
    "target.container_id = source.container_id",
    ["container_id"],
)
