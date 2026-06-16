# Data flow: EarthRanger → Gundi

The pull actions fetch raw ER records and transform them into the Gundi schema before sending. The
transforms live in `app/actions/handlers.py`; the send functions in `app/services/gundi.py`.

## Events

`transform_events_to_gundi_schema()` maps an ER event to a Gundi event:

| Gundi field | Source |
|-------------|--------|
| `title` | ER `title`, falling back to the event-type display name, then the event-type slug. |
| `event_type` | ER `event_type` slug. |
| `source` | The `event_type` slug. (ER events have no per-device source; grouping by type avoids defaulting everything to one source.) |
| `recorded_at` | ER `time`, falling back to `created_at`. |
| `location` | ER `{longitude, latitude}` → Gundi `{lon, lat}`. |
| `geometry` | ER `geojson`. |
| `event_details` | ER `event_details`. |
| `provider_metadata` | `source_event_url` deep-link + `serial_number` (when available). |
| `additional` | Any remaining ER fields not mapped above. |

### Deep-link back to ER

`provider_metadata.source_event_url` is built as `{er_ui_root}/events/{er_event_id}`, where `er_ui_root` is
derived from the integration's base URL. Downstream destinations (e.g. C-more) render it as a click-through
back to the source ER event. If the base URL can't be parsed, `er_ui_root` is empty and the deep-link is
simply omitted.

New events are sent with `send_events_to_gundi()` (a batched POST). The Gundi-assigned `object_id` from the
response is saved in per-event state so later updates can be correlated to the same event.

## Notes and field changes (event updates)

For an event seen on a previous run, `_emit_event_updates()` detects what changed and emits **one Gundi
event-update per change** (via `update_event_in_gundi()`, a PATCH). Emitting one update per logical change
means each one surfaces cleanly downstream (e.g. as a separate C-more comment) — this is the GUNDI-5386
contract.

- **New notes** — each ER note whose ID isn't in `seen_note_ids` is forwarded as
  `changes = {"notes": [note]}` (preserving author and timestamp), then its ID is recorded as seen.
- **Field changes** — detected by diffing the cached value against the current one:

  | ER field | Gundi change key |
  |----------|------------------|
  | `state` | `status` |
  | `priority` | `priority` |
  | `title` | `title` |

  Each changed field emits `changes = {<key>: <new value>}`.

!!! info "Why a note can fail to forward"
    Notes only enter this path if the event was fetched with `include_notes=true`
    (see [`pull_events`](actions/pull-events.md)). The note must also be *new* relative to `seen_note_ids` —
    notes already present when the event was first pulled are marked seen and won't backfill.

## Observations

`transform_observations_to_gundi_schema()` maps an ER observation to a Gundi observation:

| Gundi field | Source |
|-------------|--------|
| `source` | `er-src-<ER source UUID>`. |
| `recorded_at` | ER `recorded_at`. |
| `location` | ER `{longitude, latitude}` → `{lon, lat}`. |
| `additional` | Any remaining ER fields. |

Observations are sent with `send_observations_to_gundi()` (a batched POST). All Gundi send functions retry
with exponential backoff on HTTP errors.
