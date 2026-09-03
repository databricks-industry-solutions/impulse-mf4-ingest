# Troubleshooting

## `impulse_data_sources` import fails

`c_mdf_to_delta.py` `%pip`-installs **databricks-impulse** from GitHub at runtime. If the import
fails, check: (1) the ingest compute can reach `github.com`; (2) the runtime is **Python 3.12**
(serverless env v4 / DBR 16.4 LTS) — the package declares `requires-python >=3.12,<3.13` and pip
will refuse to install on 3.11; (3) the git ref in the `%pip install` line still exists (branch
`feature/mdfDataSources`, or `main` after merge).

## No files detected

Confirm MDF4 files exist under `/Volumes/<catalog>/<schema>/<mdf4_volume>/` (not only under `_seed/`). Re-run `a_demo_setup` with `use_demo_data=True` or copy files manually.

## `next_run_id` is `noop`

No `unprocessed`, reprocessable `in_progress`, or reprocessable `failed` runs exist. Check `status`:

```sql
SELECT status, COUNT(*) FROM <catalog>.<schema>.status GROUP BY status;
```

## Multiple `in_progress` runs

The batch selector raises an error when more than one distinct `in_progress` run_id exists. Fix `status` manually or mark stale rows `failed`.

## Demo timestamps look wrong

After schema or timestamp fixes, run demo setup with `reset=True` and `force_convert` (via `reset`) to regenerate MDF4 files with correct relative timestamps.

## NaN in channel metrics

Resolved in current versions: RLE stats ignore NaN values and use duration-weighted means. Reset analytics tables if upgrading from an older schema.

## Delta merge type errors

Signal **values** are stored as `float` (float32 from ingest). **Time** columns use `double` (float64) for epoch-second precision.

## GPL demo dependency

`asammdf` is used only in `demo_setup` (serverless environment). Production ingest does not require it.
