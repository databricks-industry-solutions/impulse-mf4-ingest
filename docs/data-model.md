# Data model

All tables live under `<catalog>.<schema>` (default `mda_demo.default`). Time columns in metrics tables use **epoch seconds** (float), not milliseconds.

## Operational

### `status`

Tracks per-file ingest lifecycle.

| Column | Type | Description |
|--------|------|-------------|
| `container_id` | BIGINT | Surrogate key (identity) |
| `run_id` | STRING | Batch identifier (SHA-256 of load timestamp) |
| `filename` | STRING | Full path to MDF4 file |
| `status` | STRING | `unprocessed`, `in_progress`, `succeeded`, `failed` |
| `_load_ts` | TIMESTAMP | When the file was discovered |
| `_processing_done_ts` | TIMESTAMP | Set when status becomes `succeeded` |

## Silver ingest

### `channels`

RLE-encoded signal intervals from `mdf_signals`.

| Column | Type | Cluster key |
|--------|------|-------------|
| `container_id` | BIGINT | yes |
| `channel_id` | INT | yes |
| `tstart`, `tend` | DOUBLE | epoch seconds |
| `value` | FLOAT | signal value |

### `bronze_meta`

Per-channel MDF metadata from `mdf_metadata`.

| Column | Type | Notes |
|--------|------|-------|
| `container_id` | BIGINT | |
| `channel_id` | INT | |
| `group_idx`, `channel_idx` | INT | MDF group/channel index |
| `channel_name` | STRING | |
| `unit` | STRING | |
| `filename` | STRING | Source file path |
| `header_datetime` | TIMESTAMP | MDF header start time |
| `md_comment` | STRING | JSON, Python dict, or plain text |

### `masters` (optional)

Master time base from `mdf_masters` when `write_masters=True`.

| Column | Type |
|--------|------|
| `container_id` | BIGINT |
| `group_idx` | INT |
| `timestamp` | DOUBLE | epoch seconds |

## Analytics

### `channel_tags`

| Column | Type |
|--------|------|
| `container_id` | BIGINT |
| `channel_id` | INT |
| `key` | STRING |
| `value` | STRING |

Fixed keys from `bronze_meta` columns; `md_comment` expanded to one row per JSON key (plain text → `comment`).

### `channel_metrics`

| Column | Type | Unit |
|--------|------|------|
| `container_id`, `channel_id` | BIGINT, INT | |
| `channel_name` | STRING | |
| `sample_count` | INT | |
| `min`, `max`, `mean` | FLOAT | signal value |
| `begin_ts`, `end_ts` | DOUBLE | epoch seconds |
| `duration_s` | DOUBLE | seconds |
| `sample_rate` | FLOAT | valid-duration fraction |
| `value_type` | STRING | e.g. `FLOAT` |

### `container_tags`

| Column | Type |
|--------|------|
| `container_id` | BIGINT |
| `key` | STRING |
| `value` | STRING |

Includes file path tags and `md_comment`-derived keys.

### `container_metrics`

| Column | Type | Unit |
|--------|------|------|
| `container_id` | BIGINT | |
| `vehicle_key` | STRING | from `md_comment` tag `vehicle_key`; `"unknown"` when absent |
| `start_ts`, `stop_ts` | DOUBLE | epoch seconds |
| `start_dt`, `stop_dt` | TIMESTAMP | wall-clock bounds |
| `duration_s` | DOUBLE | seconds |
| `num_channels` | INT | |

## Gold layer (reporting)

Produced by the **`MDA_Report`** job (`notebooks/reports/signal_report.py`) using the
`impulse_reporting` framework. Tables are written under `<catalog>.<schema>` with a prefix from the
`table_prefix` parameter (default `report`), as a fact/dimension **star schema**:

| Table | Kind | Contents |
|-------|------|----------|
| `report_histogram_fact` | fact | 1D histogram bins (RPM, speed) — duration-weighted |
| `report_histogram_dimension` | dim | Histogram definitions (name, bins, channel, units) |
| `report_histogram2d_fact` | fact | 2D histogram cells (RPM vs speed) |
| `report_histogram2d_dimension` | dim | 2D histogram definitions |
| `report_stats_aggregator_fact` | fact | Per-event statistics (min/median/mean/max, milestone values) |
| `report_stats_aggregator_dimension` | dim | Stats aggregator definitions |
| `report_event_dimension` | dim | Event definitions (RPM band, container, distance, milestones) |
| `report_event_instance_fact` | fact | Resolved event instances per container |
| `report_measurement_dimension` | dim | Container dimensions carried from `container_metrics` |

Key fact columns:

- `report_histogram_fact(container_id, visual_id, event_id, bin_id, lower_bound, upper_bound, bin_name, hist_value)` — `hist_value` is **microseconds** (duration in each bin).
- `report_histogram2d_fact(container_id, visual_id, event_id, x_bin_id, y_bin_id, x_lower_bound, x_upper_bound, y_lower_bound, y_upper_bound, x_bin_name, y_bin_name, hist_value)`.
- `report_stats_aggregator_fact(container_id, visual_id, channel_name, event_id, event_instance_id, aggregation_label, statistic_value)`.

Facts join to their dimensions on `visual_id`; filter a dimension by `name` (e.g.
`rpm_histogram`, `rpm_speed_heatmap`, `container_stats`, `values_at_distance_milestones`) to select a
specific visual.

**Time-unit adapter:** the impulse query engine expects `channels` timestamps in **microseconds**
(long), but the silver `channels` table stores `tstart`/`tend` as **epoch seconds** (double). The
report notebook therefore exposes a view `<catalog>.<schema>._report_channels_rle_us` that casts the
time columns to µs and runs the engine in `RLE` mode.

## Migration note

Column renames from prior versions: `begin_ms`→`begin_ts`, `end_ms`→`end_ts`, `duration_ms`→`duration_s`. Run demo setup with `reset=True` to recreate tables.
