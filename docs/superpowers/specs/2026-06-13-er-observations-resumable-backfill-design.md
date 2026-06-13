# ER Observations: Resumable Backfill, Pagination & Resilience

**Date:** 2026-06-13
**Status:** Approved (design)
**Scope:** `app/actions/handlers.py::action_pull_observations` and its helpers.
`pull_events` is explicitly out of scope (the chunking machinery is built as a
reusable helper so events can adopt it later).

## Problem

The ER connector can read observations for a wide time window across many
subjects, but in practice three failures abort or stall the process:

- **(a) Pagination of source assignments.** `_resolve_source_ids` calls
  `get_source_assignments(subject_ids=sorted(subject_ids))`, which joins *every*
  subject UUID into one `subjects=…` query string (risking a 414 URI-too-long)
  and reads only the first page of the `subjectsources` envelope via `_as_list`.
  With enough subjects, sources are silently dropped.
- **(b) Assignment-parsing errors.** An error occurs while parsing assignments;
  the exact cause is unknown. Recent commits hardened the consumer against the
  paginated-envelope crash, but we are not certain the remaining error is the
  same one. We instrument before fixing.
- **(c) Timeouts at scale.** `action_pull_observations` pulls **every**
  observation in the ER instance for the window and filters client-side by
  `source_id_set`, all inside a single `asyncio.wait_for(MAX_ACTION_EXECUTION_TIME)`
  (9 min) call. There is no mid-run cursor, so a timeout (asyncio or Cloud Run)
  loses the entire run.

### Scale

A representative backfill: ~20 subjects, 18 months, 100–500 observations per day
per subject ≈ **3.3M–5.5M observations**. At `batch_size=100` that is 30k–55k
page fetches — impossible inside one 9-minute invocation.

## Root-cause findings

- ER's observation/event generators (`get_observations`, `get_events`) paginate
  correctly via cursor pagination in `erclient`'s `_get_data`. Pagination is
  *not* broken there.
- `get_source_assignments` (subjectsources) and `get_subjectgroups` are
  single-request calls — no pagination.
- ER's observations endpoint filters by a **single** `subject_id` *or*
  `source_id` — there is no multi-source filter. The current "pull the whole
  instance, filter in Python" approach is therefore both the timeout cause and
  unnecessary work: we can loop per-source and let ER filter server-side.

## Goals

1. Fetch only the configured sources' observations (server-side filtering),
   not the whole instance.
2. Make a pull **bounded per invocation and resumable** so an arbitrarily large
   historical window completes across many invocations without losing progress.
3. Fix source-assignment pagination so no sources are dropped.
4. Capture enough diagnostics to identify the assignment-parsing error from a
   live run.

## Non-goals

- Changing `pull_events` (reuses the helper later).
- Per-observation dedup state. We accept **at-least-once** delivery and rely on
  downstream dedup (Gundi keys observations by source + `recorded_at`).
  Confirmed acceptable by the operator.
- Parallel/fan-out processing across multiple PubSub messages.

## Architecture

### Unit of work

Observations are processed as discrete **(time sub-window × source)** units:

- **Outer loop:** ascending time sub-windows over `[start, end]`.
- **Inner loop:** each resolved `source_id`, fetched with ER server-side
  filtering (`get_observations(source_id=…, start=window_start, end=window_end)`).

Outer-time / inner-source ordering keeps watermark semantics clean: everything
up to the last fully completed sub-window is done.

**Sub-window size** is operator-configurable, **default 1 day**. At 20 sources ×
~300 obs/day that is ~6k obs / ~60 pages per source-day — comfortably inside the
time budget, and a re-done unit re-sends only a few thousand observations.

### Resumable cursor

A backfill cursor is stored in Redis state (action-level, `source_id` =
default `"no-source"` slot, distinct from the per-source observation data),
recording how far the pull has progressed:

```json
{
  "backfill": {
    "window_start": "<ISO-8601 start of the sub-window in progress>",
    "source_index": <int, index into the ordered source list>,
    "window_end": "<ISO-8601 ceiling for the whole backfill>",
    "sources": ["<source_id>", "..."],
    "no_progress_count": <int>
  }
}
```

- The cursor is **committed after each `(source, sub-window)` unit fully drains.**
  On crash/timeout mid-unit, at most that one unit is redone (at-least-once).
- When the cursor reaches `window_end` across all sources, it is cleared, the
  normal `last_execution` watermark is advanced to `window_end`, and the action
  reverts to cheap incremental pulls.
- `sources` is snapshotted into the cursor at backfill start so the source list
  is stable across resumes even if assignments change mid-backfill.

### Start/resume logic

On each invocation:

1. If a live `backfill` cursor exists → **resume** from it (ignore the
   watermark; the cursor is authoritative).
2. Else compute the window as today:
   - `window_end` = `pull_config.end_datetime` or execution timestamp.
   - `window_start` = `pull_config.start_datetime` if no watermark or
     `force_run_since_start`, else `last_execution`.
   - Resolve sources, snapshot the new cursor.

Incremental pulls are the degenerate case: a window small enough to finish all
units in one invocation, which then advances the watermark and clears the cursor
in the same run.

### Time budget

- Soft budget = `MAX_ACTION_EXECUTION_TIME * BUDGET_FRACTION` (default 0.8).
- Track `time.monotonic()` from handler start. **Before** starting each unit,
  if elapsed ≥ soft budget, commit the cursor and return
  `{"status": "in_progress", ...}`.
- The existing `asyncio.wait_for(MAX_ACTION_EXECUTION_TIME)` in
  `action_runner.py` stays as the hard backstop. A unit hard-killed mid-drain
  simply leaves the cursor at the last committed boundary → redone next time.

### Continuation across invocations

- **Default — scheduler-driven.** Returning `in_progress` leaves the cursor in
  place; the next scheduled tick detects it and resumes. A large backfill
  catches up over many ticks. Naturally rate-limited, no new infrastructure, no
  runaway risk. Requires `run_on_schedule` to be on for the backfill to progress.
- **Opt-in — self-re-trigger.** A new config flag (e.g.
  `continue_immediately`, default `False`) makes the action call
  `trigger_action(integration_id, "pull_observations")` (republish to
  `INTEGRATION_COMMANDS_TOPIC`) at the end of a budget-bounded run so the next
  chunk starts immediately. Guard: track `no_progress_count` in the cursor and
  stop re-triggering after `MAX_NO_PROGRESS_RETRIES` (default 3) consecutive
  units that advance the cursor by zero, to prevent an always-failing unit from
  looping forever. Requires `INTEGRATION_COMMANDS_TOPIC` configured.

### Concurrency: overlapping runs

Pull actions are scheduled type-wide and fire on every tick. A budget-bounded
chunk run (or an in-flight self-re-trigger) can still be executing when the next
scheduled tick fires, producing two concurrent invocations of the same
`(integration, pull_observations)` against the **same cursor** — concurrent
duplicate sends plus a cursor-write race (one clobbering the other → rework or a
skipped window). This is worse than the at-least-once we accept for crashes, so
it must be excluded.

**Fix — a Redis lease (mutual exclusion).** Reuse
`IntegrationStateManager.set_if_absent` (Redis `SET … NX EX`):

- Before reading/advancing the cursor, acquire
  `set_if_absent(integration_id, "pull_observations", source_id="backfill-lock",
  ttl_seconds=MAX_ACTION_EXECUTION_TIME + LOCK_MARGIN_SECONDS)`.
- **Not acquired** → another invocation owns it → return
  `_skip_quietly(reason="backfill_in_progress")`. No cursor read, no work, no
  double-send. The next tick retries.
- **Release in a `finally`** on every exit (complete *and* `in_progress`), so the
  next tick / self-re-trigger picks up immediately.
- The TTL is the crash backstop. Because `asyncio.wait_for` hard-kills the
  handler at `MAX_ACTION_EXECUTION_TIME`, a run can never outlive its own lease,
  so the `finally` delete is always safe (it can only ever delete its own lease);
  if the process dies outright, the TTL expires the lease so the backfill never
  wedges. `LOCK_MARGIN_SECONDS` (default 30) keeps the TTL strictly above the
  hard timeout.
- Best-effort, fail-safe acquisition: if Redis is unavailable when acquiring,
  follow the existing throttle precedent — log and proceed (a missed lease is a
  rare duplicate, not a crash). The release is likewise best-effort.

Overlapping ticks thus become harmless no-ops; at most one invocation advances
the cursor at a time, and at-least-once remains the worst case (crash only,
never overlap).

## Source-assignment pagination fix (a)

Replace the single giant call in `_resolve_source_ids` with a chunked helper:

- Chunk `subject_ids` into groups of `SUBJECT_ID_CHUNK_SIZE` (default 25) and
  call `get_source_assignments` once per chunk. Small chunks keep each URL short
  **and** keep each result within one page, sidestepping the lack of pagination
  on `subjectsources` without reaching into erclient internals.
- Aggregate the resulting `source_id`s across chunks into the set.
- **Defensive detection:** if any chunk response still carries a `next` (the
  single-page assumption breaking), emit a WARNING activity-log entry so the
  blind spot is visible rather than silent.

## Diagnostics for assignment parsing (b)

Instrument the subjectsources consumer (the `_as_list(assignments)` path in
`_resolve_source_ids`) to capture, on each chunk:

- The raw response **type** (`dict` envelope vs flat list vs unexpected).
- Envelope `count` / presence of `next`, and the parsed record count.
- A **sample record** (first element) for shape inspection.
- Any record that fails the `isinstance(a, dict) and a.get("source")` check,
  logged with `extra={"attention_needed": True}`.
- A single summarizing activity-log entry per run when anomalies are seen.

Logging is structured and bounded (sample, not full dump) so it is safe on a
live run. We fix the root cause once a real failure is captured.

## New settings / config

| Name | Location | Default | Purpose |
|------|----------|---------|---------|
| `subwindow_days` | `PullObservationsConfig` | 1 | Time sub-window size for chunking. |
| `continue_immediately` | `PullObservationsConfig` | `False` | Opt-in self-re-trigger via PubSub. |
| `BUDGET_FRACTION` | handler constant / setting | 0.8 | Fraction of `MAX_ACTION_EXECUTION_TIME` before yielding. |
| `SUBJECT_ID_CHUNK_SIZE` | handler constant | 25 | Subjects per `subjectsources` request. |
| `MAX_NO_PROGRESS_RETRIES` | handler constant | 3 | Self-re-trigger runaway guard. |
| `LOCK_MARGIN_SECONDS` | handler constant | 30 | Lease TTL margin above the hard timeout. |

## Return shape

`action_pull_observations` returns one of:

- In progress: `{"status": "in_progress", "observations_extracted": <run total>,
  "window_start": ..., "source_index": ..., "sources_resolved": N}`
- Complete: `{"status": "complete", "observations_extracted": <run total>,
  "filter_active": bool, "sources_resolved": N}`
- Skipped (existing behavior preserved): zero-source / no-resolvable cases.

## Error handling

- Per-unit fetch/transform errors are logged with `attention_needed` and do
  **not** advance the cursor past the failing unit; the run continues to the
  next unit where safe, or yields, so a fix can re-pull.
- The watermark is advanced **only** on full window completion — preserving the
  existing "don't lose the window on failure" guarantee.
- Existing skip/zero-source/no-resolvable-source behavior is unchanged.

## Testing

- **Pagination fix:** subject_ids chunked correctly; sources aggregated across
  chunks; `next`-present chunk emits the warning.
- **Diagnostics:** anomalous subjectsources shapes produce the expected logs
  without raising.
- **Chunking/cursor:** a window spanning multiple sub-windows produces the
  expected unit sequence; cursor committed at unit boundaries; resume from a
  mid-window cursor continues correctly; watermark advanced and cursor cleared
  only on completion.
- **Time budget:** a mocked clock past the soft budget yields `in_progress`
  with a committed cursor.
- **Continuation:** scheduler-driven resume picks up the cursor; opt-in
  self-re-trigger calls `trigger_action` and stops after
  `MAX_NO_PROGRESS_RETRIES`.
- **Server-side filtering:** `get_observations` called per-source with
  `source_id` and the sub-window `start`/`end` (no whole-instance pull).
- **Concurrency lease:** a second invocation while the lease is held returns
  `skipped: backfill_in_progress` and touches neither cursor nor ER; the lease is
  released on both `complete` and `in_progress` exits and on a simulated timeout.
- All external services (ER client, Gundi, Redis, PubSub) mocked per existing
  `app/conftest.py` conventions.

## Risks & open items

- **Downstream dedup assumption.** At-least-once re-sends rely on Gundi keying
  observations by source + `recorded_at`. Confirmed acceptable; flagged here as
  the one external dependency to validate if duplicates appear.
- **Per-request read timeout.** `DEFAULT_REQUESTS_TIMEOUT` read=20s; erclient
  retries up to 5×. If ER is slow under load, consider raising the read timeout;
  out of scope unless live data shows it.
