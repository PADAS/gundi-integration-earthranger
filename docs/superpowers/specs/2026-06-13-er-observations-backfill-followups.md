# ER Observation Backfill — Review Follow-ups

**Source:** Code review of PR #30 (`feat/resumable-observation-backfill`)
**Date:** 2026-06-13
**Related:** spec `2026-06-13-er-observations-resumable-backfill-design.md`, plan `docs/superpowers/plans/2026-06-13-resumable-observation-backfill.md`

These are non-blocking follow-ups identified during review of PR #30. None is a
correctness blocker; the core loop/cursor/lease/watermark machinery is sound and
covered by tests. They are recorded here so they aren't lost.

---

## 1. Confirm the watermark semantics change (needs a second human's sign-off)

**What changed:** A successful pull now advances `last_execution` to
`cursor["end"]` (= `end_datetime`, or the first invocation's timestamp) instead
of always to "now".

**Why it's likely an improvement:** The old behavior was a latent gap bug — a
bounded backfill `[Jan, Feb]` run in June set the watermark to June, silently
skipping Feb–June. The new behavior advances only to the end of the window that
was actually pulled.

**The flip side to confirm acceptable:** With a fixed `end_datetime` left in
place, every later run recomputes an empty window and logs an empty completion,
and the watermark can sit at a past ceiling. If the operator later clears
`end_datetime`, the next run re-pulls from that ceiling to now (at-least-once;
downstream dedup absorbs it). Documented in the `end_datetime` field
description, but the semantics change deserves an explicit reviewer sign-off
since this is a self-reviewed change.

**Action:** Confirm intended; no code change expected.

---

## 2. Make result accounting consistent across a multi-invocation backfill (minor)

`units_failed` is cumulative (stored in the cursor), but `observations_extracted`
is per-invocation (a local that resets each run). A completed multi-chunk
backfill therefore returns e.g. `{observations_extracted: 50, units_failed: 12}`
where `12` is total-across-backfill but `50` is only the final chunk — easy to
misread.

**Action:** Either accumulate `observations_extracted` in the cursor (like
`units_failed`) so both are cumulative, or relabel both as per-invocation.

---

## 3. Document/fix `continue_immediately` ↔ `run_on_schedule` coupling (minor)

`trigger_action` publishes a `RunIntegrationAction` with no `triggered_by`, so
the re-triggered chunk is treated as **AUTO**. In `execute_action`, an AUTO pull
with `run_on_schedule=False` is skipped. So a *manual* first run with
`continue_immediately=True` and `run_on_schedule=False` stalls silently after the
first chunk. In the intended usage (`run_on_schedule=True`) it works.

**Action:** Either note "requires `run_on_schedule`" in the `continue_immediately`
field description, or have the continuation pass a marker so it isn't skippable.

---

## 4. Acquire the lease before opening the ER client (minor)

The lease is acquired *inside* `async with er_client`, so an overlapping run that
will skip still opens the ER client first (likely a token fetch/auth round-trip)
before discovering the lease is held.

**Action:** Acquire the lease before entering the client context so a skipped
overlapping run is truly cheap. Low impact (skips are infrequent).

---

## 5. Note the atomic-unit granularity limit (minor)

A single `(source × sub-window)` unit drains all its pages with no mid-unit
budget check, and `subwindow_days` has `ge=1` but no upper bound. If one unit
exceeds `MAX_ACTION_EXECUTION_TIME` it is hard-killed and re-runs indefinitely
(the `no_progress_count` guard only brakes the self-re-trigger path, not the
scheduler-driven path). For the stated scale (~6k obs/source-day) this is
comfortably safe; `subwindow_days` is the lever.

**Action:** Add a one-line comment that the unit is the atomic granularity and
`subwindow_days` bounds it; optionally consider a sane upper bound or a mid-unit
budget check if very high-volume sources appear.
