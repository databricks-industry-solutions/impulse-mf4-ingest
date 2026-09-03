# Databricks notebook source
# MAGIC %md
# MAGIC ![image](../../docs/flow_d.png)
# MAGIC
# MAGIC ### Generate channel-level tags and metrics from `channels` / `bronze_meta` and write results to Delta tables.
# MAGIC * Fixed tags from `bronze_meta` columns (`group_idx`, `channel_name`, …)
# MAGIC * RLE `channels` stats ignore NaN; `mean` is duration-weighted, `sample_rate` is valid-time fraction

# COMMAND ----------

# MAGIC %run ./bundle_bootstrap

# COMMAND ----------

import pyspark.sql.functions as F

import utils
from utils import schema_definitions
from utils.notebook_helpers import (
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

channel_tags_table = f"{catalog}.{schema}.channel_tags"
channel_metrics_table = f"{catalog}.{schema}.channel_metrics"
channels_table = f"{catalog}.{schema}.channels"
bronze_meta_table = f"{catalog}.{schema}.bronze_meta"

print("channel_tags_table", channel_tags_table)
print("channel_metrics_table", channel_metrics_table)

# COMMAND ----------

current_run_id = dbutils.jobs.taskValues.get(taskKey="get_next_batch", key="next_run_id", debugValue="NA")
batch = get_batch_context(status_table_df, current_run_id)
open_container_ids = batch["open_container_ids"]

print("number of open files to convert:", len(batch["open_files"]))
if not batch["open_files"]:
  dbutils.notebook.exit("Nothing to do.")

# COMMAND ----------

channels_df = (
    spark.read.table(channels_table)
    .where(F.col("container_id").isin(open_container_ids))
)

meta_df = (
    spark.read.table(bronze_meta_table)
    .where(F.col("container_id").isin(open_container_ids))
)

# COMMAND ----------

channel_names_df = meta_df.select("container_id", "channel_id", "channel_name").distinct()

_val = schema_definitions.TIMESERIES_VALUE_SQL_TYPE
_time = schema_definitions.TIMESERIES_TIME_SQL_TYPE

channels_prepared = (
    channels_df
    .withColumn("time_valid", is_finite(F.col("tstart")) & is_finite(F.col("tend")))
    .withColumn(
        "interval_duration",
        finite_or_null(
            F.when(
                F.col("time_valid"),
                (F.col("tend") - F.col("tstart")).cast(_time),
            )
        ),
    )
    .withColumn("value_valid", is_finite(F.col("value")))
)

channel_metrics_df = (
    channels_prepared
    .groupBy("container_id", "channel_id")
    .agg(
        F.sum(F.when(F.col("value_valid"), F.lit(1)).otherwise(F.lit(0))).cast("int").alias("sample_count"),
        F.min(F.when(F.col("value_valid"), F.col("value"))).cast(_val).alias("min"),
        F.max(F.when(F.col("value_valid"), F.col("value"))).cast(_val).alias("max"),
        F.try_divide(
            F.sum(
                F.when(
                    F.col("value_valid") & F.col("time_valid"),
                    F.col("value") * F.col("interval_duration"),
                ).otherwise(F.lit(0))
            ),
            F.sum(
                F.when(
                    F.col("value_valid") & F.col("time_valid"),
                    F.col("interval_duration"),
                ).otherwise(F.lit(0))
            ),
        ).cast(_val).alias("mean"),
        F.min(F.when(F.col("time_valid"), F.col("tstart"))).cast(_time).alias("begin_ts"),
        F.max(F.when(F.col("time_valid"), F.col("tend"))).cast(_time).alias("end_ts"),
        F.sum(
            F.when(
                F.col("value_valid") & F.col("time_valid"),
                F.col("interval_duration"),
            ).otherwise(F.lit(0))
        ).cast(_time).alias("valid_duration"),
    )
    .join(channel_names_df, on=["container_id", "channel_id"], how="left")
    .withColumn(
        "duration_s",
        finite_or_null(
            F.when(
                is_finite(F.col("begin_ts")) & is_finite(F.col("end_ts")),
                (F.col("end_ts") - F.col("begin_ts")).cast(_time),
            )
        ),
    )
    .withColumn(
        "sample_rate",
        F.try_divide(F.col("valid_duration"), F.col("duration_s")).cast(_val),
    )
    .withColumn("value_type", F.lit("FLOAT"))
    .select(
        "container_id",
        "channel_id",
        "channel_name",
        "sample_count",
        finite_or_null(F.col("min")).cast(_val).alias("min"),
        finite_or_null(F.col("max")).cast(_val).alias("max"),
        finite_or_null(F.col("mean")).cast(_val).alias("mean"),
        finite_or_null(F.col("begin_ts")).cast(_time).alias("begin_ts"),
        finite_or_null(F.col("end_ts")).cast(_time).alias("end_ts"),
        finite_or_null(F.col("duration_s")).cast(_time).alias("duration_s"),
        finite_or_null(F.col("sample_rate")).cast(_val).alias("sample_rate"),
        "value_type",
    )
)

# COMMAND ----------

_FIXED_CHANNEL_TAGS = [
    "group_idx",
    "channel_idx",
    "channel_name",
    "unit",
    "header_datetime",
]

tag_cols = [F.col(tag).cast("string").alias(tag) for tag in _FIXED_CHANNEL_TAGS]
fixed_channel_tags_df = (
    meta_df.select("container_id", "channel_id", *tag_cols)
    .unpivot(["container_id", "channel_id"], _FIXED_CHANNEL_TAGS, "key", "value")
)

comment_channel_tags_df = md_comment_tags_df(meta_df, ["container_id", "channel_id"])
channel_tags_df = fixed_channel_tags_df.unionByName(comment_channel_tags_df)

# COMMAND ----------

utils.merge_or_append_table(
    spark,
    channel_metrics_df,
    channel_metrics_table,
    "target.container_id = source.container_id and target.channel_id = source.channel_id",
    ["container_id", "channel_id"],
)
utils.merge_or_append_table(
    spark,
    channel_tags_df,
    channel_tags_table,
    "target.container_id = source.container_id and target.channel_id = source.channel_id and target.key = source.key",
    ["container_id", "channel_id"],
)
