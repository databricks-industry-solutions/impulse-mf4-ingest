# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [Unreleased]

### Added

- Shared utility modules: `constants`, `delta_ops`, `notebook_helpers`, `mdf_comments`, `demo_conversion`
- Documentation: `docs/architecture.md`, `data-model.md`, `deployment.md`, `parameters.md`, `troubleshooting.md`
- Bundle compute: dev serverless + ingest job cluster; prod job cluster (DBR 16.4 LTS / Python 3.12)
- CI: bundle validate, ruff, pytest for pure-Python helpers
- GitHub issue/PR templates, `CODE_OF_CONDUCT.md`
- `MDA_Report` reporting job (`notebooks/reports/signal_report.py`) — builds a Gold-layer star schema
  (`report_*`) with the `databricks-impulse` reporting framework (histograms, RPM-vs-speed heatmap,
  per-event stats, 10 km milestones)
- `MDF4 Signal Report` AI/BI dashboard (`dashboards/MDF_Report.lvdash.json`) deployed with `MDA_Report`
- `warehouse_name` bundle variable (lookup → `warehouse_id`) backing the report dashboard

### Changed

- MDF4 data sources now sourced from the [`databricks-impulse`](https://github.com/databrickslabs/impulse)
  package (`impulse_data_sources.mdf`), `%pip`-installed from GitHub by `c_mdf_to_delta.py`
  (pinned to `feature/mdfDataSources` until merged), replacing the vendored `modules/mdf/` copy
- Prod ingest cluster runtime bumped to DBR 16.4 LTS (Python 3.12) for `databricks-impulse` compatibility
- **Breaking:** Renamed metric time columns to reflect epoch seconds: `begin_ts`, `end_ts`, `duration_s` (was `*_ms`)
- Refactored failed-run rollback into `rollback_batch_tables`
- Consolidated status counts in `b_get_next_batch`
- File sizes resolved on driver instead of executor UDF
- Delta `targetFileSize` set once at table creation in demo setup
- Parallel `channel_metadata` and `container_metadata` after ingest

### Removed

- `d_analytical_layer.py` (silver written directly in ingest)
- `initial_setup.py`, duplicate `src/ingest_mdf/` copy
- Per-run `ALTER TABLE` in `c_mdf_to_delta`
- Unused `checkpoint_directory` widget and `signal_replication_factor`

### Fixed

- Prod `write_masters` smart-quote typo in bundle variables
- Spark date format for stable `run_id` (`yyyy-MM-dd HH:mm:ss.SSS`)
- Demo MDF timestamps (relative master + `header.start_time`)
- NaN handling in channel metrics (duration-weighted stats)
- Delta merge float32/double mismatch on `value` column

## [0.0.1]

### Added

- Initial public release
