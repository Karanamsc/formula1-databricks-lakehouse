# Full Refresh Pipeline

Reprocesses the entire landing dataset on every run. Simplest version of the Formula 1 medallion pipeline.

## Configuration

Set in `00-common/01.environment-config.py`:

| Setting | Value |
|---|---|
| Catalog | `databricks_dev` |
| Schema | `formula1` |
| Landing path | `/Volumes/databricks_dev/formula1/landing` |

Change these before running in another environment.

## Expected landing files

`circuits.csv`, `races.csv`, `constructors.json`, `drivers.json`, and the `results/` and `sprints/` folders.

## Notebooks (run in this order)

| Folder | Notebooks |
|---|---|
| `00-common` | `01.environment-config`, `02.bronze-helpers` (included via `%run`) |
| `01-bronze` | `01. Bronze`: ingests all landing files to `bronze_*` Delta tables |
| `02-silver` | `01`–`06` Transform Circuits / Races / Constructors / Drivers / Results / Sprints |
| `03-gold` | `91.Build Nationality Region Reference` first, then `01` Races dim, `02` Constructors dim, `03` Drivers dim, `04` Results fact |
| `04-analytics` | `01`/`02` driver and constructor standings views; `03`/`04` dominant drivers/constructors analysis and `91` demo SQL (`.dbquery.ipynb`) |

The `91` notebooks are reference or demo notebooks. The gold nationality reference feeds the constructors and drivers dimensions.

## Running as a job

Use [`../jobs/job_formula1_lakehouse_full_refresh.json`](../jobs/job_formula1_lakehouse_full_refresh.json). It wires the notebooks above as a dependency graph (bronze → silver → gold). Update the `notebook_path` values to your workspace.
