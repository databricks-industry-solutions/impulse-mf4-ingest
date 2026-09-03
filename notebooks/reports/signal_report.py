# Databricks notebook source
# MAGIC %md
# MAGIC # Impulse — Reporting Pipeline
# MAGIC
# MAGIC A complete reporting pipeline built on the ingested MDF4 tables with the **databricks-impulse**
# MAGIC reporting framework (TSAL): RPM histograms, an RPM-vs-speed heatmap, per-distance-bin
# MAGIC statistics, and channel values sampled at every 10 km milestone — persisted as a Gold-layer
# MAGIC star schema and visualized inline with matplotlib.
# MAGIC
# MAGIC Adapted from `databrickslabs/impulse` `demos/reporting_pipeline.ipynb`. The demo's data-setup
# MAGIC (loading curated CSVs into a silver layer) is replaced here by the accelerator's own ingest
# MAGIC output: `channels`, `channel_metrics`, `container_metrics`, `channel_tags`, `container_tags`.
# MAGIC
# MAGIC **One adaptation vs. the demo:** the accelerator stores `channels` as RLE intervals with
# MAGIC `tstart`/`tend` in **epoch seconds** (double), whereas the impulse query engine works in
# MAGIC **microseconds** (long). We expose a thin view that casts the time columns to µs and run the
# MAGIC engine in `RLE` mode (the demo used `RAW` because its channels were raw point samples).

# COMMAND ----------

# MAGIC %md
# MAGIC #### Install the `databricks-impulse` reporting framework
# MAGIC Same pre-release GitHub pin as the ingest job — switch the ref to `@main` after the branch
# MAGIC merges (or to `databricks-impulse` once published to PyPI). `pydantic`/`scipy` back the
# MAGIC reporting engine (pre-installed on serverless; pinned here to match the impulse demo).

# COMMAND ----------

# MAGIC %pip install --quiet "git+https://github.com/databrickslabs/impulse.git@feature/mdfDataSources" "pydantic>=2.0" scipy

# COMMAND ----------

# MAGIC %restart_python

# COMMAND ----------

# MAGIC %md # 1. Configure source & sink

# COMMAND ----------

import warnings

warnings.filterwarnings("ignore", category=UserWarning)

dbutils.widgets.text("catalog", "mda_demo", "Catalog")
dbutils.widgets.text("schema", "default", "Schema")
dbutils.widgets.text("table_prefix", "report", "Gold table prefix")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
TABLE_PREFIX = dbutils.widgets.get("table_prefix")
if not CATALOG or not SCHEMA:
    raise ValueError("Please set the catalog and schema widgets.")

pfx = f"{CATALOG}.{SCHEMA}.{TABLE_PREFIX}"
print(f"Source: {CATALOG}.{SCHEMA}.*   Gold sink: {pfx}_*")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Adapt the silver `channels` to the query engine's schema
# MAGIC The engine expects RLE `channels` as `(container_id, channel_id, tstart:long, tend:long,
# MAGIC value:double)` in **microseconds**. The accelerator writes `tstart`/`tend` as epoch **seconds**
# MAGIC (double), so we expose a view that casts to µs. Everything else (`channel_metrics`,
# MAGIC `container_metrics`, tags) is consumed as-is.

# COMMAND ----------

CHANNELS_VIEW = f"{CATALOG}.{SCHEMA}._report_channels_rle_us"
spark.sql(
    f"""
    CREATE OR REPLACE VIEW {CHANNELS_VIEW} AS
    SELECT
        container_id,
        channel_id,
        CAST(ROUND(tstart * 1e6) AS BIGINT) AS tstart,
        CAST(ROUND(tend   * 1e6) AS BIGINT) AS tend,
        CAST(value AS DOUBLE)               AS value
    FROM {CATALOG}.{SCHEMA}.channels
    """
)
print(f"Created µs-adapter view: {CHANNELS_VIEW}")

# COMMAND ----------

# MAGIC %md
# MAGIC # 2. Initialize the Report
# MAGIC The `Report` orchestrator takes a config specifying the silver **source** tables, the Gold
# MAGIC **unity_sink**, the `query_engine` solver + data type (`RLE` for our interval channels), and
# MAGIC the container `measurement_dimensions` carried into the Gold layer.

# COMMAND ----------

import pyspark.sql.functions as F
from databricks.sdk import WorkspaceClient

from impulse_reporting.aggregations.histogram import HistogramDuration
from impulse_reporting.aggregations.histogram2d import Histogram2DDuration
from impulse_reporting.aggregations.point_value_aggregator import PointValueAggregator
from impulse_reporting.aggregations.stats_aggregator import StatsAggregator
from impulse_reporting.core.page import Page
from impulse_reporting.core.report import Report
from impulse_reporting.events.basic_event import BasicEvent
from impulse_reporting.events.container_event import ContainerEvent
from impulse_reporting.events.points_in_time_event import PointsInTimeEvent

ws = WorkspaceClient()

config = {
    "source": {
        "container_metrics_table": f"{CATALOG}.{SCHEMA}.container_metrics",
        "channel_metrics_table": f"{CATALOG}.{SCHEMA}.channel_metrics",
        "channels_uri": CHANNELS_VIEW,
        "container_tags_table": f"{CATALOG}.{SCHEMA}.container_tags",
        "channel_tags_table": f"{CATALOG}.{SCHEMA}.channel_tags",
    },
    "unity_sink": {
        "catalog": CATALOG,
        "schema": SCHEMA,
        "table_prefix": TABLE_PREFIX,
    },
    "query_engine": {
        "solver": "DefaultSolver",
        "data_type": "RLE",
    },
    "measurement_dimensions": [
        "container_id",
        "vehicle_key",
        "start_ts",
        "stop_ts",
    ],
}

report = Report(name="mdf_report", spark=spark, config=config, workspace_client=ws)
db = report.get_db()
print("Report initialized")

# COMMAND ----------

# MAGIC %md
# MAGIC # 3. Select physical channels
# MAGIC Channels are selected by **metadata tags** — no column names, no SQL, no joins. These are
# MAGIC **lazy expressions**: no data is read yet. (The ingested `channel_tags` carry `channel_name`,
# MAGIC `brand`, and `model`, matching the demo's selectors.)

# COMMAND ----------

eng_rpm = db.query.channel(channel_name="Engine RPM", brand="Seat", model="Leon")
veh_spd = db.query.channel(channel_name="Vehicle Speed Sensor", brand="Seat", model="Leon")
amb_air_temp = db.query.channel(channel_name="Ambient Air Temperature", brand="Seat", model="Leon")
intake_air_temp = db.query.channel(
    channel_name="Intake Air Temperature", brand="Seat", model="Leon"
)

# COMMAND ----------

# MAGIC %md
# MAGIC # 4. Define virtual signals & events
# MAGIC **TSAL** uses Python operators to build lazy expression trees. Virtual signals derive from
# MAGIC physical channels; events are time windows (or instants) where a condition holds. Time is in
# MAGIC microseconds, so `resample(1e6)` resamples to 1 s and the distance integral divides by
# MAGIC 3600 (h→s) and 1e6 (µs→s).

# COMMAND ----------

avg_temp = (amb_air_temp + intake_air_temp) / 2
distance_km = veh_spd.resample(1e6).cumtrapz() / 3600 / 1e6

rpm_band = (eng_rpm > 2000) & (eng_rpm < 5000)
every_10km = (distance_km % 10).intervals_between_falling_edges()

# Instant the trip odometer crosses each additional 10 km — a set of points in time.
distance_milestones = (distance_km % 10).falling_edges()

# COMMAND ----------

# MAGIC %md
# MAGIC # 5. Register events
# MAGIC - **BasicEvent** — from a TSAL boolean expression
# MAGIC - **ContainerEvent** — spans the entire recording
# MAGIC - **PointsInTimeEvent** — a set of instants (e.g. each 10 km milestone)

# COMMAND ----------

rpm_event = BasicEvent(
    name="rpm_event",
    expr=rpm_band,
    desc="Engine RPM between 2000 and 5000",
    required_channels=["Engine RPM"],
)
report.add_event(rpm_event)

container_event = ContainerEvent(name="container_event", desc="Full measurement recording")
report.add_event(container_event)

distance_event = BasicEvent(name="distance_event", expr=every_10km, desc="Every 10 km driven")
report.add_event(distance_event)

milestone_event = PointsInTimeEvent(
    name="distance_milestones", expr=distance_milestones, desc="Each 10 km driven (instant)"
)
report.add_event(milestone_event)

# COMMAND ----------

# MAGIC %md
# MAGIC # 6. Define aggregations
# MAGIC - **Histogram** — 1D duration-weighted distribution
# MAGIC - **Histogram2D** — 2D heatmap of two signals
# MAGIC - **StatsAggregator** — min, median, mean, max per event
# MAGIC - **PointValueAggregator** — channel value sampled at each instant of a points-in-time event

# COMMAND ----------

page = Page(page_number=1)
report.add_page(page)

sigs = [eng_rpm, veh_spd, amb_air_temp, intake_air_temp, avg_temp]
names = ["Engine RPM", "Vehicle Speed", "Ambient Air Temp", "Intake Air Temp", "Avg Temp"]
aggs = ["min", "median", "mean", "max"]

page.add_aggregation(
    HistogramDuration(
        name="rpm_histogram",
        base_expr=eng_rpm,
        bins=[float(i) for i in range(0, 5000, 250)],
        event=rpm_event,
        desc="RPM distribution within RPM events",
        channel_name="Engine RPM",
        bins_unit="RPM",
        values_unit="s",
    )
)
page.add_aggregation(
    HistogramDuration(
        name="speed_histogram",
        base_expr=veh_spd,
        bins=[float(i) for i in range(0, 200, 10)],
        event=rpm_event,
        desc="Speed distribution within RPM events",
        channel_name="Vehicle Speed",
        bins_unit="km/h",
        values_unit="s",
    )
)
page.add_aggregation(
    Histogram2DDuration(
        name="rpm_speed_heatmap",
        x_expr=eng_rpm,
        y_expr=veh_spd,
        x_bins=[float(i) for i in range(2000, 5000, 250)],
        y_bins=[float(i) for i in range(0, 200, 10)],
        event=rpm_event,
        desc="RPM vs Speed heatmap",
        x_channel_name="Engine RPM",
        y_channel_name="Vehicle Speed",
        x_bins_unit="RPM",
        y_bins_unit="km/h",
        values_unit="s",
    )
)
page.add_aggregation(
    StatsAggregator(
        name="rpm_event_stats",
        input_expressions=sigs,
        channel_names=names,
        statistics=aggs,
        event=rpm_event,
        desc="Statistics within RPM events",
    )
)
page.add_aggregation(
    StatsAggregator(
        name="container_stats",
        input_expressions=sigs,
        channel_names=names,
        statistics=aggs,
        event=container_event,
        desc="Statistics for full measurement",
    )
)
page.add_aggregation(
    StatsAggregator(
        name="distance_stats",
        input_expressions=sigs + [distance_km],
        channel_names=names + ["Distance"],
        statistics=aggs,
        event=distance_event,
        desc="Statistics per 10 km distance bin",
    )
)
page.add_aggregation(
    PointValueAggregator(
        name="values_at_distance_milestones",
        input_expressions=[veh_spd, eng_rpm],
        channel_names=["Vehicle Speed", "Engine RPM"],
        event=milestone_event,
        desc="Speed & RPM at each 10 km milestone",
    )
)
print(f"{len(page.aggregations)} aggregations added")

# COMMAND ----------

# MAGIC %md
# MAGIC # 7. Compute & persist
# MAGIC - `determine_report()` — parallel per-container execution
# MAGIC - `persist_results()` — writes the Gold-layer star schema

# COMMAND ----------

report.determine_report()
report.persist_results()
print(f"Report persisted to {pfx}_*")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7b. Publish Unity Catalog metric views
# MAGIC A governed semantic layer over the Gold `report_*` tables — one metric view per report type
# MAGIC (histogram, 2D histogram, stats). Consumers (e.g. the MDF4 Signal Report dashboard) query
# MAGIC these with `MEASURE(...)` instead of re-joining fact/dimension tables.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {CATALOG}.{SCHEMA}.mv_report_histogram WITH METRICS LANGUAGE YAML AS $$
version: 0.1
source: |
  SELECT f.lower_bound, f.bin_name, f.hist_value, d.name AS histogram_name
  FROM {pfx}_histogram_fact AS f
  JOIN {pfx}_histogram_dimension AS d ON f.visual_id = d.visual_id
dimensions:
  - name: histogram_name
    expr: histogram_name
  - name: lower_bound
    expr: lower_bound
  - name: bin_name
    expr: bin_name
measures:
  - name: total_duration_s
    expr: SUM(hist_value) / 1000000
$$
""")

spark.sql(f"""
CREATE OR REPLACE VIEW {CATALOG}.{SCHEMA}.mv_report_heatmap WITH METRICS LANGUAGE YAML AS $$
version: 0.1
source: {pfx}_histogram2d_fact
dimensions:
  - name: x_lower_bound
    expr: x_lower_bound
  - name: y_lower_bound
    expr: y_lower_bound
  - name: x_bin_name
    expr: x_bin_name
  - name: y_bin_name
    expr: y_bin_name
measures:
  - name: total_duration_s
    expr: SUM(hist_value) / 1000000
$$
""")

spark.sql(f"""
CREATE OR REPLACE VIEW {CATALOG}.{SCHEMA}.mv_report_stats WITH METRICS LANGUAGE YAML AS $$
version: 0.1
source: |
  SELECT f.container_id, f.channel_name, f.aggregation_label, f.event_instance_id, f.statistic_value, d.name AS stats_name
  FROM {pfx}_stats_aggregator_fact AS f
  JOIN {pfx}_stats_aggregator_dimension AS d ON f.visual_id = d.visual_id
dimensions:
  - name: stats_name
    expr: stats_name
  - name: container_id
    expr: container_id
  - name: channel_name
    expr: channel_name
  - name: aggregation_label
    expr: aggregation_label
  - name: event_instance_id
    expr: event_instance_id
measures:
  - name: stat_value
    expr: ANY_VALUE(statistic_value)
  - name: milestone_speed
    expr: MAX(CASE WHEN channel_name = 'Vehicle Speed' THEN statistic_value END)
  - name: milestone_rpm
    expr: MAX(CASE WHEN channel_name = 'Engine RPM' THEN statistic_value END)
$$
""")

print(f"Published metric views under {CATALOG}.{SCHEMA}: mv_report_histogram, mv_report_heatmap, mv_report_stats")

# COMMAND ----------

# MAGIC %md
# MAGIC # 8. Visualize the results
# MAGIC Read the Gold-layer tables back and render inline with **matplotlib**:
# MAGIC - **Bar** — RPM histogram
# MAGIC - **Heatmap** — RPM vs Speed
# MAGIC - **Table** — per-container statistics
# MAGIC - **Scatter** — Speed & RPM at each 10 km milestone

# COMMAND ----------

import matplotlib.pyplot as plt

T = f"{pfx}"

# 1. BAR — RPM Histogram (aggregated across all containers)
hist_df = (
    spark.read.table(f"{T}_histogram_fact")
    .join(
        spark.read.table(f"{T}_histogram_dimension").filter("name = 'rpm_histogram'"),
        on="visual_id",
    )
    .groupBy("bin_id", "lower_bound", "upper_bound", "bin_name")
    .agg(F.sum("hist_value").alias("total_duration_us"))
    .orderBy("bin_id")
    .toPandas()
)
hist_df["duration_s"] = hist_df["total_duration_us"] / 1e6

fig, ax = plt.subplots(figsize=(10, 4))
ax.bar(hist_df["bin_name"], hist_df["duration_s"], color="steelblue", edgecolor="white")
ax.set_xlabel("Engine RPM bin")
ax.set_ylabel("Duration (s)")
ax.set_title("RPM Histogram — Duration in Each RPM Band (all containers)")
plt.xticks(rotation=45, ha="right", fontsize=8)
plt.tight_layout()
plt.show()

# COMMAND ----------

# 2. HEATMAP — RPM vs Speed
heat_df = (
    spark.read.table(f"{T}_histogram2d_fact")
    .groupBy(
        "x_bin_id", "y_bin_id", "x_bin_name", "y_bin_name", "x_lower_bound", "y_lower_bound"
    )
    .agg(F.sum("hist_value").alias("total_us"))
    .toPandas()
)
heat_df["duration_s"] = heat_df["total_us"] / 1e6

pivot = heat_df.pivot_table(
    index="y_bin_id", columns="x_bin_id", values="duration_s", fill_value=0
)
x_labels = sorted(
    heat_df[["x_bin_id", "x_bin_name"]].drop_duplicates().values, key=lambda r: r[0]
)
y_labels = sorted(
    heat_df[["y_bin_id", "y_bin_name"]].drop_duplicates().values, key=lambda r: r[0]
)

fig, ax = plt.subplots(figsize=(10, 6))
im = ax.imshow(pivot.values, aspect="auto", origin="lower", cmap="YlOrRd", interpolation="nearest")
ax.set_xticks(range(len(x_labels)))
ax.set_xticklabels([lbl[1] for lbl in x_labels], rotation=45, ha="right", fontsize=7)
ax.set_yticks(range(len(y_labels)))
ax.set_yticklabels([lbl[1] for lbl in y_labels], fontsize=7)
ax.set_xlabel("Engine RPM")
ax.set_ylabel("Vehicle Speed (km/h)")
ax.set_title("RPM vs Speed Heatmap — Duration (s)")
plt.colorbar(im, ax=ax, label="Duration (s)")
plt.tight_layout()
plt.show()

# COMMAND ----------

# 3. TABLE — Per-container statistics (container_stats)
stats_df = (
    spark.read.table(f"{T}_stats_aggregator_fact")
    .join(
        spark.read.table(f"{T}_stats_aggregator_dimension").filter("name = 'container_stats'"),
        on="visual_id",
    )
    .select("container_id", "channel_name", "aggregation_label", "statistic_value")
    .toPandas()
)
stats_pivot = stats_df.pivot_table(
    index=["container_id", "channel_name"], columns="aggregation_label", values="statistic_value"
).reset_index()
stats_pivot.columns.name = None
stats_pivot = stats_pivot.sort_values(["container_id", "channel_name"])

print("Per-Container Statistics (full measurement):")
display(
    spark.createDataFrame(
        stats_pivot[["container_id", "channel_name", "min", "median", "mean", "max"]]
    )
)

# COMMAND ----------

# 4. SCATTER — Speed & RPM at each 10 km milestone
milestone_df = (
    spark.read.table(f"{T}_stats_aggregator_fact")
    .join(
        spark.read.table(f"{T}_stats_aggregator_dimension").filter(
            "name = 'values_at_distance_milestones'"
        ),
        on="visual_id",
    )
    .select("container_id", "channel_name", "event_instance_id", "statistic_value")
    .toPandas()
)
milestone_pivot = milestone_df.pivot_table(
    index=["container_id", "event_instance_id"], columns="channel_name", values="statistic_value"
).reset_index()

fig, ax = plt.subplots(figsize=(10, 5))
for cid, grp in milestone_pivot.groupby("container_id"):
    ax.scatter(
        grp["Vehicle Speed"],
        grp["Engine RPM"],
        label=f"Container {cid}",
        s=60,
        alpha=0.8,
        edgecolors="k",
        linewidths=0.5,
    )
ax.set_xlabel("Vehicle Speed (km/h)")
ax.set_ylabel("Engine RPM")
ax.set_title("Speed & RPM at Each 10 km Milestone")
plt.tight_layout()
plt.show()
