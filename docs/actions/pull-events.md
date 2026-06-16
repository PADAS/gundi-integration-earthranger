# `pull_events`

Pulls events from EarthRanger and forwards them to Gundi — both **new events** and, for events already
seen, **updates** (new notes and changed fields).

`action_pull_events` — `app/actions/handlers.py`

## What it does

1. **Resolve the time window.** On the first run (or when `force_run_since_start` is set) it starts at
   `start_datetime`; otherwise it starts at the watermark saved from the last successful run. The window
   is applied to the ER timestamp named by `filter_date_field` (default `updated_at`). See
   [State & scheduling](../state-and-scheduling.md).
2. **Resolve filters.** Configured `event_types` / `event_categories` slugs are resolved to ER UUIDs
   (ER filters by UUID, not slug). If some slugs don't resolve, it logs a warning; if a configured filter
   resolves to *nothing*, it **skips the pull without advancing the watermark** so a corrected config can
   re-pull the same window.
3. **Fetch events** from the ER events endpoint in batches of 100, with `include_notes=True` so each event
   carries its notes (without this, note updates never fire — see the note below).
4. **Per event, decide new vs. update** using per-event state keyed by the ER event UUID:
      - **Never seen** → transform and POST a new Gundi event, record the returned `gundi_object_id`, and
        mark all current notes as already-seen (so existing notes aren't bulk-forwarded on first sight).
      - **Seen before** → if `updated_at` is unchanged, skip; otherwise emit **one Gundi event-update per
        change** (each new note, and each changed `status` / `priority` / `title`).
5. **Advance the watermark** to the run's start time once all events are processed.

It returns counts: `events_extracted`, `events_updated`, `updates_emitted`, `events_skipped_unchanged`.

!!! note "`include_notes` is load-bearing"
    ER's events-*list* endpoint omits the notes array unless `include_notes=true` is requested. The action
    passes it explicitly; without it, every event arrives note-less and no note update is ever emitted —
    the ER-note → downstream-comment path silently never fires. See the
    [data flow](../data-flow.md#notes-and-field-changes-event-updates).

## Configuration — `PullEventsConfig`

| Field | Type | Default | Notes |
|-------|------|---------|-------|
| `start_datetime` | ISO-8601 string | **required** | First-run window start; ignored after the watermark advances (unless `force_run_since_start`). |
| `end_datetime` | ISO-8601 string | none | Optional ceiling; sent on every run. Leave empty for ongoing pulls; set only for bounded backfills. |
| `filter_date_field` | enum: `updated_at` / `created_at` / `event_time` | `updated_at` | Which ER timestamp the window applies to. `updated_at` catches edits and backdated events; `event_time` can silently miss backdated events — use it only for bounded backfills. |
| `force_run_since_start` | bool | `False` | Reset the watermark for one run. Toggle off after the catch-up, or every run re-pulls from `start_datetime`. |
| `event_types` | list[str] | `[]` | ER event-type slugs to pull (e.g. `wildlife_sighting_rep`). Empty = no type filter. Find slugs via [`show_permissions`](show-permissions.md). |
| `event_categories` | list[str] | `[]` | ER event-category slugs. Combined with types using ER's AND semantics. Empty = no category filter. |
| `run_on_schedule` | bool | `False` | Enable scheduled pulling. Off by default — turn on per connection that should pull events. |
