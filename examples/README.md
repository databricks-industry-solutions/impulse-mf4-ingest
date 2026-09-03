# Examples

## Demo data

`obd_dataset.zip` — OBD-II CSV archive from [KIT RADAR](https://doi.org/10.35097/1130), converted to MDF4 by `a_demo_setup.py`.

## Sample queries

Import the `.dbquery.ipynb` notebooks into your workspace and replace placeholders:

- `{{catalog}}` — your catalog (default `mda_demo`)
- `{{schema}}` — your schema (default `default`)

| Notebook | Description |
|----------|-------------|
| `BasicExampleQuery.dbquery.ipynb` | Time-series plot data for Engine RPM |
| `BasicExampleHistogramQuery.dbquery.ipynb` | Value distribution histogram |

## Dashboards

- **MDF4 Signal Report** — [`../dashboards/MDF_Report.lvdash.json`](../dashboards/MDF_Report.lvdash.json). Deployed automatically by the bundle with the `MDA_Report` job. Two pages: **Signal Report** (silver metrics — drives, signals, durations) and **Report Results** (the Gold `report_*` tables — RPM/speed histograms, RPM-vs-speed heatmap, per-drive stats, 10 km milestones). Its dataset queries reference `mda_demo.default.*`; edit the JSON if you deploy elsewhere.
