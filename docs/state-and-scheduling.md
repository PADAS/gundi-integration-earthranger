# State & scheduling

Both pull actions are incremental: each run only fetches what's new since the last one. They achieve this
with state stored in Redis by `IntegrationStateManager` (`app/services/state.py`).

## The state store

State is JSON, stored in `REDIS_STATE_DB` (default DB 0) under keys of the form:

```
integration_state.{integration_id}.{action_id}.{source_id}
```

`source_id` defaults to `"no-source"` when a single record covers the whole action. The API is small:
`get_state` (returns `{}` on miss), `set_state`, `delete_state`, and `set_if_absent` — an atomic
set-with-TTL used for the backfill lease. All calls retry on transient Redis errors.

## Watermarks

Each pull action tracks a `last_execution` watermark:

| Situation | `last_execution` | Window start used |
|-----------|------------------|-------------------|
| First run | absent | `start_datetime` from config |
| Subsequent run | present | `last_execution` |
| Catch-up (`force_run_since_start = True`) | present | `start_datetime` (reset for one run) |

`force_run_since_start` resets the watermark for a single run — **toggle it off afterward**, or every run
re-pulls from `start_datetime`. `end_datetime`, when set, is sent on every run; clear it after a bounded
backfill so later runs don't recompute an empty window.

For `pull_events`, the watermark is compared against the ER timestamp chosen by `filter_date_field`
(default `updated_at`, which catches edits and backdated events).

## Per-event state (`pull_events`)

`pull_events` keeps one record per ER event, keyed by the **ER event UUID**:

| Field | Purpose |
|-------|---------|
| `gundi_object_id` | The Gundi ID returned when the event was first posted — used to correlate later updates. |
| `updated_at` | Cached ER `updated_at`; if unchanged on the next sight, the event is skipped. |
| `state`, `priority`, `title` | Cached values, diffed to detect field changes. |
| `seen_note_ids` | IDs of notes already forwarded, so only *new* notes emit updates. |

On first sight, all existing notes are recorded as seen (no bulk-forward). See
[Data flow](data-flow.md#notes-and-field-changes-event-updates).

## The resumable observation backfill

`pull_observations` is built to backfill large windows across many runs without dropping or duplicating
data. It stores a **cursor** under its state alongside `last_execution`:

```python
{
  "start": ..., "end": ...,          # the overall window
  "subwindow_days": ...,             # slice width
  "sources": [...],                  # sorted source UUIDs ([None] = no filter)
  "window_index": ..., "source_index": ...,  # progress within the (sub-window × source) grid
  "no_progress_count": ...,          # runaway guard for continue_immediately
  "units_failed": ...,
}
```

The window is sliced into deterministic, half-open `[start, end)` sub-windows (`_iter_subwindows`), and the
action walks the `(sub-window × source)` grid, committing the cursor after each unit. Because the source
list is sorted and the slicing is deterministic, a resumed run regenerates the exact same unit sequence and
continues by index.

The watermark (`last_execution`) is **advanced only when the whole window completes** — so a run that fails
or times out mid-backfill leaves the watermark untouched and the next run resumes from the cursor.

### Time budget and continuation

At ~80% of `MAX_ACTION_EXECUTION_TIME`, the run saves its cursor and stops. Continuation is either:

- **Scheduler-driven** (default) — the next scheduled tick picks up the cursor, or
- **Immediate** (`continue_immediately = True`) — the run re-triggers the next chunk via the
  `INTEGRATION_COMMANDS_TOPIC` PubSub topic. A runaway guard stops self-re-triggering after 3 consecutive
  runs that make no progress.

### The backfill lease

Because a backfill can outlast its schedule interval, two runs could otherwise overlap and race on the
cursor (causing duplicate sends). Before working, the action takes a lease via `set_if_absent` with a TTL of
`MAX_ACTION_EXECUTION_TIME + 30s`. If the lease is already held, the run skips quietly. The lease is released
in a `finally` block, with the TTL as a backstop. Lease acquisition **fails open** — if Redis is briefly
unavailable, the run proceeds rather than crash (a rare duplicate is cheaper than a stall).

## Scheduling and `run_on_schedule`

Schedules are attached with `@crontab_schedule("…")` or `register.py --schedule`. But both pull actions
default `run_on_schedule` to **`False`**: a scheduled tick for a pull action whose config has
`run_on_schedule` off (or missing/invalid config) is **skipped quietly**, not errored. This integration is
most often deployed only as a *destination*, so pulling is opt-in per connection. Manual runs always
execute regardless.
