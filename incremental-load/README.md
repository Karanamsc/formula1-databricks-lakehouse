# Incremental Load Pipeline

Processes one landing batch at a time. Batches are sub-folders of the landing volume, and their status is tracked in a control table so each batch runs once.

See [`../docs/architecture/`](../docs/architecture/) for the processing diagrams.

## Configuration

Set in `00-common/01.environment-config.py`:

| Setting | Value |
|---|---|
| Catalog | `databricks-dev` (hyphenated, so it needs backtick quoting in SQL) |
| Schema | `formula1_incr` |
| Control schema | `control` |
| Landing path | `/Volumes/databricks-dev/formula1_incr/landing` |

## Notebooks

| Folder | Notebooks |
|---|---|
| `00-common` | `01.environment-config`, `02.bronze-helpers`, `03.silver-helpers`, `04.gold-helpers` |
| `01-bronze` | `01. Bronze` |
| `02-silver` | `01`–`06` Transform Circuits / Races / Constructors / Drivers / Results / Sprints |
| `03-gold` | `91.Build Nationality Region Reference`, `01`–`03` dimensions, `04` Results fact |
| `04-analytics` | `01`/`02` driver and constructor standings views |
| `05-orchestration` | Batch control (see below) |

## Batch orchestration

1. `00.Create Control Tables`: creates `control.batch_control` (`batch_id`, `status`, `created_timestamp`, `updated_timestamp`). Run once.
2. `01.Identify Next Batch`: lists landing batch folders, subtracts those already `in_progress` or `completed`, and sets the task values `p_batch_id` and `has_batch`.
3. `02.Create New Batch`: registers the batch as `in_progress`.
4. Run the refresh job on that batch.
5. `03.Complete Batch`: marks the batch `completed`.

## Jobs

| File | Role |
|---|---|
| [`job_formula1_incremental_batch_orchestration.json`](../jobs/job_formula1_incremental_batch_orchestration.json) | Top-level job: identify batch → check one exists → create batch → run the refresh job → complete batch |
| [`job_formula1_lakehouse_incremental_refresh.json`](../jobs/job_formula1_lakehouse_incremental_refresh.json) | Bronze → silver → gold for the selected batch |

Update the `notebook_path` values to your workspace before importing.

## Known issue

In `05-orchestration/01.Identify Next Batch.py`, the `else` branch assigns `tracked_batch = []` (missing the `s`), but the code afterwards uses `tracked_batches`. On the first run, before the control table exists, this raises a `NameError`. Fix the variable name, or run `00.Create Control Tables` first.
