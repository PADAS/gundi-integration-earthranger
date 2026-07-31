# `pull_observations`

Pulls subject tracking observations (positions over time) from EarthRanger and forwards them to Gundi.
Designed to backfill large historical windows safely and resumably.

`action_pull_observations` — `app/actions/handlers.py`

## What it does

1. **Acquire a backfill lease.** Because a pull can run longer than its schedule interval, the action takes
   a Redis lease first; if another run already holds it, this run skips quietly. The lease is released in a
   `finally` block (and has a TTL as backstop). See
   [State & scheduling](../state-and-scheduling.md#the-backfill-lease).
2. **Resolve the time window** from the watermark / `start_datetime` (same semantics as
   [`pull_events`](pull-events.md)).
3. **Resolve sources.** The configured `subject_group_ids` are walked recursively (a parent group includes
   its sub-groups), the member subjects are collected, and their current **source** assignments are
   resolved (chunked to keep ER URLs short). An empty group list means "no source filter."
4. **Process the window as `(source × sub-window)` units.** The window is sliced into `subwindow_days`-wide
   sub-windows; for each source and sub-window it fetches observations (batch size 100), transforms them,
   and POSTs to Gundi. Progress is committed to a **cursor** after each unit.
5. **Respect a time budget.** At ~80% of `MAX_ACTION_EXECUTION_TIME` the run saves its cursor and stops.
   The next scheduled tick resumes from the saved cursor — or, if `continue_immediately` is on, the run
   re-triggers the next chunk immediately via PubSub (with a runaway guard that stops after 3 consecutive
   no-progress runs).
6. **Advance the watermark** only when the entire window is complete.

Observations are transformed with `external_source_id = manufacturer_id` (falling back to `er-src-{uuid}`), `source_name = time-accurate Subject.name`, and `subject_type`; location is mapped to `{lon, lat}`; and all other ER fields are carried in `additional`. See [Data flow](../data-flow.md#observations).

## Configuration — `PullObservationsConfig`

| Field | Type | Default | Notes |
|-------|------|---------|-------|
| `start_datetime` | ISO-8601 string | **required** | First-run window start; ignored after the watermark advances (unless `force_run_since_start`). |
| `end_datetime` | ISO-8601 string | none | Optional ceiling; sent on every run. Clear it after a bounded backfill, or later runs recompute an empty window. |
| `subject_group_ids` | list[str] | `[]` | ER subject-group UUIDs to include (recursively). Empty = no constraint. Find UUIDs via [`show_permissions`](show-permissions.md). |
| `subwindow_days` | int | `1` | Backfill granularity: the window is processed in slices this many days wide, committing after each. Smaller = less re-work on resume; larger = less per-slice overhead. |
| `force_run_since_start` | bool | `False` | Reset the watermark for one run. Toggle off after the catch-up. |
| `continue_immediately` | bool | `False` | When a run hits its time budget with work left, self-re-trigger the next chunk via PubSub instead of waiting for the next tick. Requires `INTEGRATION_COMMANDS_TOPIC`. |
| `run_on_schedule` | bool | `False` | Enable scheduled pulling. Off by default. |
