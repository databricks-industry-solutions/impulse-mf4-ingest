# Contributing

We welcome contributions to this Databricks Solution Accelerator.

## Before you start

- Review the [Databricks License](LICENSE.md) and ensure your employer permits contribution under its terms.
- Sign the [Databricks Contributor License Agreement](https://cla.databricks.com/) (CLA) before your first PR is merged.

## Development setup

```bash
pip install -e ".[dev]"
ruff check modules/
pytest tests/
databricks bundle validate -e dev
databricks bundle validate -e prod
```

## Pull request checklist

- [ ] Changes are scoped to the described problem
- [ ] `ruff check modules/` passes
- [ ] `pytest tests/` passes (for helper changes)
- [ ] `databricks bundle validate` passes for dev and prod
- [ ] Documentation updated (`README.md`, `docs/`, or `CHANGELOG.md` as appropriate)
- [ ] `CHANGELOG.md` updated under `[Unreleased]` for user-visible changes

## Coding style

- Match existing notebook and Python conventions in `modules/utils/`
- Prefer extending shared helpers over duplicating notebook logic
- Keep demo-only GPL dependencies (`asammdf`) isolated to `demo_setup`

## Reporting issues

Use the GitHub issue templates for bugs and feature requests. For security issues, see [SECURITY.md](SECURITY.md).
