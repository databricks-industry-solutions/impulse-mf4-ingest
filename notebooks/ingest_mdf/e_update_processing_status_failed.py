# Databricks notebook source
# MAGIC %md
# MAGIC <img src="../../docs/flow_enable.png" width="560" height="275">
# MAGIC
# MAGIC ### Update workflow run status to 'failed' in the status table.
# MAGIC * Trigger: on-failure step in the ingestion workflow
# MAGIC * Rolls back partial writes for the current batch and marks the run failed

# COMMAND ----------

# MAGIC %run ./bundle_bootstrap

# COMMAND ----------

import utils
from utils.constants import STATUS_FAILED
from utils.notebook_helpers import (
    get_batch_context,
    read_catalog_schema_widgets,
    require_status_table,
)

# COMMAND ----------

catalog, schema = read_catalog_schema_widgets(dbutils)
status_table_df = require_status_table(spark, dbutils, catalog, schema)

current_run_id = dbutils.jobs.taskValues.get(taskKey="get_next_batch", key="next_run_id", debugValue="NA")
batch = get_batch_context(status_table_df, current_run_id)
open_container_ids = batch["open_container_ids"]

# COMMAND ----------

utils.rollback_batch_tables(spark, catalog, schema, open_container_ids)
utils.update_status(spark, f"{catalog}.{schema}.status", current_run_id, STATUS_FAILED)
