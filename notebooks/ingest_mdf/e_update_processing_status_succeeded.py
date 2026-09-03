# Databricks notebook source
# MAGIC %md
# MAGIC <img src="../../docs/flow_enable.png" width="560" height="275">
# MAGIC
# MAGIC ### Update files for succeeded conversions

# COMMAND ----------

# MAGIC %run ./bundle_bootstrap

# COMMAND ----------

import utils
from utils.constants import STATUS_SUCCEEDED
from utils.notebook_helpers import read_catalog_schema_widgets

# COMMAND ----------

catalog, schema = read_catalog_schema_widgets(dbutils)
status_table_name = f"{catalog}.{schema}.status"
current_run_id = dbutils.jobs.taskValues.get(taskKey="get_next_batch", key="next_run_id", debugValue="NA")

# COMMAND ----------

utils.update_status(spark, status_table_name, current_run_id, STATUS_SUCCEEDED)

# COMMAND ----------

utils.optimize_tables(spark, catalog, schema)
print("ALL DONE")
