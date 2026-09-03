# Databricks notebook source
# MAGIC %md
# MAGIC <img src="../../docs/flow_b.png" width="560" height="275">
# MAGIC
# MAGIC ### Detect any new files that need to be processed
# MAGIC  
# MAGIC * Checks `mdf4_volume` directory for new files
# MAGIC * Creates entries in the `status` table to track status of MDF4 files

# COMMAND ----------

# MAGIC %run ./bundle_bootstrap

# COMMAND ----------

import pyspark.sql.functions as F

from utils.notebook_helpers import is_uc_volume_path, resolve_volume_path

# COMMAND ----------

# DBTITLE 1,Define widgets
dbutils.widgets.text("mdf4_volume", "mdf4", "Source Volume")
dbutils.widgets.text("checkpoint_volume", "mdf4_checkpoint", "Checkpoint Volume")
dbutils.widgets.text("catalog", "mda_demo")
dbutils.widgets.text("schema", "default")
dbutils.widgets.text("max_batch_size", "100")

# COMMAND ----------

# DBTITLE 1,Resolve runtime parameters
# Resolve widget values and derive commonly used paths/table names
mdf4_volume = dbutils.widgets.get("mdf4_volume")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
source_directory = resolve_volume_path(catalog, schema, mdf4_volume)
checkpoint_volume = dbutils.widgets.get("checkpoint_volume")
checkpoint_directory = f"{resolve_volume_path(catalog, schema, checkpoint_volume)}/mdf"
max_batch_size = int(dbutils.widgets.get("max_batch_size"))
status_table_name = f"{catalog}.{schema}.status"
print('Running on source_directory:', source_directory)
print('Running on checkpoint_volume:', checkpoint_volume)

# COMMAND ----------

# DBTITLE 1,Detect new files
# Define streaming source with Auto Loader (cloudFiles) to detect new files only.
# We are not parsing file contents; we only capture metadata needed for workflow:
# - filename: normalized path (remove dbfs: prefix)
# - status: initial processing state "unprocessed"
# - _load_ts: ingestion timestamp
# - run_id: deterministic batch identifier derived from timestamp string
new_files_df = (spark.readStream
  .format('cloudFiles')
  .option("cloudFiles.format", "binaryFile")  # list files regardless of content
  .option('cloudFiles.maxFilesPerTrigger', max_batch_size)
  .load(source_directory)
  .withColumn('filename', F.regexp_replace(F.col("path"), "dbfs:", ""))
  .withColumn("status", F.lit("unprocessed"))
  .withColumn("_load_ts", F.now())
  .withColumn("ts_str", F.date_format(F.col("_load_ts"), "yyyy-MM-dd HH:mm:ss.SSS"))
  .withColumn("run_id", F.sha2(F.col("ts_str"), 256))
  .select('filename', 'status', '_load_ts', 'run_id')
)

# COMMAND ----------

# DBTITLE 1,Create new batch
# Write discovered files to the status table with checkpointing for exactly-once
# semantics. availableNow triggers a bounded run that processes current backlog
# and then exits when no more data is available.
query = (new_files_df
 .writeStream
 .option("checkpointLocation", checkpoint_directory)
 .outputMode("append")
 .trigger(availableNow=True)
 .toTable(status_table_name)
 )

# COMMAND ----------

# Block until the bounded stream completes
query.processAllAvailable()
