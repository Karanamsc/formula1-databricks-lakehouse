# Formula 1 Lakehouse on Databricks

A medallion-architecture (bronze → silver → gold → analytics) data pipeline for Formula 1 data, built with PySpark and Delta Lake on Azure Databricks using Unity Catalog.

The repo contains two implementations of the same pipeline:

| Folder | Approach | Summary |
|---|---|---|
| [`full-refresh/`](full-refresh/README.md) | Full refresh | Reprocesses the whole landing dataset on every run. |
| [`incremental-load/`](incremental-load/README.md) | Incremental | Processes one landing batch at a time, tracked in a control table. |

## Repository layout

```
formula1-databricks/
├── docs/
│   ├── architecture/   # medallion / incremental processing diagrams
│   ├── erd/            # raw, silver and gold entity-relationship diagrams
│   └── results/        # output charts (standings, dominant drivers/constructors)
├── full-refresh/       # notebooks for the full-refresh pipeline
├── incremental-load/   # notebooks for the incremental pipeline
└── jobs/               # exported Databricks job definitions (JSON)
```

## Layers

| Layer | Folder | Purpose |
|---|---|---|
| Common | `00-common` | Environment config and shared helper notebooks, pulled in with `%run`. |
| Bronze | `01-bronze` | Ingest raw landing files (CSV/JSON) to Delta with source-file and ingestion-timestamp metadata. |
| Silver | `02-silver` | Clean and conform circuits, races, constructors, drivers, results, sprints. |
| Gold | `03-gold` | Dimensional model: races, constructors and drivers dimensions plus a results fact. |
| Analytics | `04-analytics` | Driver and constructor standings views and analysis. |

Diagrams are in [`docs/`](docs/).

## Jobs

Job definitions are in [`jobs/`](jobs/). They are Databricks exports, so notebook paths point at the original workspace location.

| File | Used by |
|---|---|
| `job_formula1_lakehouse_full_refresh.json` | `full-refresh/` |
| `job_formula1_lakehouse_incremental_refresh.json` | `incremental-load/` (the per-batch pipeline) |
| `job_formula1_incremental_batch_orchestration.json` | `incremental-load/` (picks the next batch, runs the refresh job, marks it complete) |

Schedules in the exports are `PAUSED`.

## Getting started

1. Clone this repo into your Databricks workspace (Workspace → Create → Git folder).
2. Create the Unity Catalog catalog, schema and landing volume referenced in each project's `00-common/01.environment-config` notebook.
3. Upload the source files to the landing volume.
4. Import a job from `jobs/` (update the notebook paths to your workspace), or run the notebooks in layer order.

## Notes

- Notebooks are Databricks source-format `.py` files; the SQL analysis notebooks are `.ipynb`.
- Do not commit credentials. Use Databricks secret scopes if any are needed.
- The two projects use different catalog and schema names (see each README), so they can coexist in one workspace.
