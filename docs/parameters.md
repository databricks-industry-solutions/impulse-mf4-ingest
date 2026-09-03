# Job parameters

Parameters are passed as Databricks job task widgets. Defaults come from [`resources/ingest_job.yml`](../resources/ingest_job.yml) and [`databricks.yml`](../databricks.yml) bundle variables.

## Unity Catalog

| Parameter | Default | Description |
|-----------|---------|-------------|
| `catalog` | `mda_demo` | Target catalog |
| `schema` | `default` | Target schema |
| `mdf4_volume` | `mdf4` | UC volume name (or full `/Volumes/...` path) |
| `checkpoint_volume` | `mdf4_checkpoint` | Auto Loader checkpoint volume name or full `/Volumes/...` path |

## Demo setup (`a_demo_setup`)

| Parameter | Default | Description |
|-----------|---------|-------------|
| `reset` | `False` | Demo only: `DROP SCHEMA CASCADE` and recreate tables |
| `use_demo_data` | `True` (dev) | `False` in prod — no deletes, no OBD import |
| `demo_data_path` | bundle `examples/obd_dataset.zip` | OBD archive or extracted directory |
| `max_batch_size` | `100` | Max files per Auto Loader micro-batch |

## Batch selection (`b_get_next_batch`)

| Parameter | Default | Description |
|-----------|---------|-------------|
| `reprocess_current_run` | `True` | Re-run a single `in_progress` run |
| `reprocess_last_failed_run` | `True` | Re-run the latest `failed` run |

When no batch is available, `next_run_id` is set to `noop` and ingest tasks are skipped via `check_data_availability`.

## MDF ingest (`c_mdf_to_delta`)

| Parameter | Default | Description |
|-----------|---------|-------------|
| `write_masters` | `False` (dev), `True` (prod) | Write `mdf_masters` to `masters` table |
| `partitioning` | `group` (dev), `stripe` (prod) | `mdf_signals` partition strategy |
| `target_partition_mb` | `64` (dev), `128` (prod) | Target output size per Spark task |
| `stripe_target_mb` | `128` (dev), `256` (prod) | Stripe size when `partitioning=stripe` |
| `max_groups_per_partition` | `64` | Cap on coalesced MDF groups per task |

## Task values

| Key | Producer | Consumers |
|-----|----------|-----------|
| `next_run_id` | `get_next_batch` | All downstream ingest and status tasks |

## Report job (`MDA_Report`)

Widgets on `notebooks/reports/signal_report.py`. `catalog`/`schema` are passed by the job from bundle variables ([`resources/report_job.yml`](../resources/report_job.yml)); `table_prefix` uses the notebook widget default.

| Parameter | Default | Source | Description |
|-----------|---------|--------|-------------|
| `catalog` | `mda_demo` | job (bundle var) | Catalog holding the ingested silver tables (report input) |
| `schema` | `default` | job (bundle var) | Schema holding the ingested silver tables |
| `table_prefix` | `report` | notebook widget | Prefix for the Gold-layer `report_*` output tables |

## Bundle variables

Defined in [`databricks.yml`](../databricks.yml); override with `--var=<name>=<value>`.

| Variable | Default | Description |
|----------|---------|-------------|
| `warehouse_name` | `Shared Unity Catalog Serverless` | Name of the SQL warehouse backing the `MDF_Report` dashboard, resolved to `warehouse_id` by lookup. Must resolve for **any** `deploy`/`run` (the dashboard is validated with the whole bundle). Override per workspace. |
| `catalog` / `schema` | `mda_demo` / `default` | Default UC location for both jobs |
| `spark_version` | `16.4.x-scala2.12` | Prod ingest job-cluster runtime (must be Python 3.12) |
