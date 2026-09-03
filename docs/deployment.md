# Deployment

## Prerequisites

- Databricks workspace with Unity Catalog
- Databricks CLI v0.200+ with bundle support
- Compute running **Python 3.12** (`databricks-impulse` requires `>=3.12,<3.13`): serverless env v4 (dev) or DBR 16.4 LTS (prod)
- Outbound network access to GitHub from the ingest compute (`databricks-impulse` is `%pip`-installed from the repo at runtime — see below)

## Bundle targets

```bash
databricks bundle validate -e dev
databricks bundle validate -e prod
databricks bundle validate -e prod_interactive --var="cluster_id=0000-000000-00000000"
databricks bundle deploy -e dev
databricks bundle run -e dev MDA_Demo
```

### Dev (`-e dev`)

- **Serverless** for all job tasks via `dev_env` (environment version `4`, Python 3.12)
- **`asammdf`** in the serverless environment (demo MDF generation only)
- **`databricks-impulse`** is `%pip`-installed from GitHub by `c_mdf_to_delta.py` (not bundled)

### Prod (`-e prod`)

- All ingest tasks run on a **job cluster** (`ingest_cluster`), default DBR **16.4 LTS** (Python 3.12)
- **`databricks-impulse`** is `%pip`-installed from GitHub by `c_mdf_to_delta.py`
- Defaults: `use_demo_data=False`, `partitioning=stripe`, larger partition sizes, `write_masters=True`
- Configure `spark_version` (must be Python 3.12) and `node_type_id` for your cloud region

### Prod interactive (`-e prod_interactive`)

- Same defaults as **prod** (`use_demo_data=False`, stripe partitioning, `write_masters=True`)
- All notebook tasks run on an **existing cluster** you provide via `cluster_id` (must be Python 3.12)
- Deploys to your user workspace path (required for `mode: production`)
- **`databricks-impulse`** is `%pip`-installed from GitHub by `c_mdf_to_delta.py`

```bash
databricks bundle deploy -e prod_interactive --var="cluster_id=<your-cluster-id>"
databricks bundle run -e prod_interactive MDA_Demo --var="cluster_id=<your-cluster-id>"
```

## databricks-impulse

The MDF4 Spark data sources ship in [`databrickslabs/impulse`](https://github.com/databrickslabs/impulse),
which is **not yet on PyPI**. The `c_mdf_to_delta.py` notebook installs it at runtime with a
notebook-scoped `%pip install` straight from the repo, currently pinned to the
`feature/mdfDataSources` branch:

```python
%pip install "git+https://github.com/databrickslabs/impulse.git@feature/mdfDataSources"
```

This works identically on serverless (dev) and classic clusters (prod), and requires the
ingest compute to have outbound access to GitHub. **Once the branch is merged**, change the
ref in `c_mdf_to_delta.py` to `@main` (or pin a release tag); once the package is published,
replace the spec with `databricks-impulse`.

`databricks-impulse` requires **Python 3.12** (`>=3.12,<3.13`) — use serverless env v4 (dev)
or DBR 16.4 LTS (prod).

## Report job (`MDA_Report`) and dashboard

The bundle also defines a second, independent job, `MDA_Report`, plus the **MDF4 Signal Report**
AI/BI dashboard (`resources/report_job.yml`).

- **Compute:** serverless in every target (Python 3.12). `databricks-impulse` is `%pip`-installed by
  `notebooks/reports/signal_report.py`, which uses the impulse **reporting framework** to build a
  Gold-layer star schema (`report_*` tables, default `table_prefix=report`) and render visualizations.
- **Run it after ingest** — it reads the silver tables produced by `MDA_Demo`:

  ```bash
  databricks bundle run -e dev MDA_Report
  ```

### Dashboard warehouse (`warehouse_name`)

The dashboard needs a SQL warehouse, resolved **by name** via the `warehouse_name` bundle variable
(default `"Shared Unity Catalog Serverless"`, resolved to `warehouse_id` by lookup). Because the whole
bundle — including the dashboard — is validated on **every** command, this warehouse must resolve for
any `deploy`/`run`, **including `MDA_Demo`**. If your workspace uses a different warehouse, override the
name:

```bash
databricks bundle deploy -e dev --var=warehouse_name="<your warehouse>"
databricks bundle run    -e dev MDA_Report --var=warehouse_name="<your warehouse>"
```

> **Note:** the dashboard's dataset queries reference `mda_demo.default.*` directly — bundle
> `${var.*}` placeholders are **not** substituted inside `.lvdash.json`. If you deploy to a different
> catalog/schema, edit the table names in `dashboards/MDF_Report.lvdash.json`. The `signal_report`
> notebook itself is parameterized via `catalog`/`schema` widgets.

## First run (demo)

```bash
# 1. Deploy both jobs + the dashboard
databricks bundle deploy -e dev
# 2. Ingest first (clean reset) — creates catalog/schema/volumes, stages OBD data, populates silver
databricks bundle run -e dev MDA_Demo --params reset=True
# 3. Then run the report — builds the Gold star schema the dashboard reads
databricks bundle run -e dev MDA_Report
```

Then open the **MDF4 Signal Report** dashboard (workspace *Dashboards*, or via
`databricks bundle summary -e dev`). Its "Report Results" page has data only after `MDA_Report` runs;
the "Signal Report" page works as soon as ingest completes.

## Live mode

Deploy with `use_demo_data=False` (prod default). The job never drops schemas or tables; `reset` is ignored. Point `mdf4_volume` at your production MDF4 volume path.
