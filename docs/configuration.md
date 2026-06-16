# Configuration reference

Every action's config model lives in `app/actions/configurations.py`. Each is configured per-connection in
the Gundi portal. This page collects every field in one place; the per-action pages add behavioral context.

## `AuthenticateConfig` — `auth`

| Field | Type | Default | Notes |
|-------|------|---------|-------|
| `authentication_type` | enum: `token`, `username_password` | `token` | Selects which credential fields apply. |
| `token` | secret string | — | Used when type is `token`. |
| `username` | string | — | Used when type is `username_password`. |
| `password` | secret string | — | Used when type is `username_password`. |

The ER **base URL** comes from the integration record, not this config.

## `ShowPermissionsConfig` — `show_permissions`

| Field | Type | Default | Notes |
|-------|------|---------|-------|
| `include_subjects_from_subgroups_in_parent` | bool | `True` | Include sub-group subjects when listing a parent group's subjects. |

## `PullEventsConfig` — `pull_events`

| Field | Type | Default | Notes |
|-------|------|---------|-------|
| `start_datetime` | ISO-8601 string | **required** | First-run window start (applies to `filter_date_field`). Ignored once the watermark advances. |
| `end_datetime` | ISO-8601 string | none | Optional window ceiling; sent every run. |
| `filter_date_field` | enum: `updated_at`, `created_at`, `event_time` | `updated_at` | Which ER timestamp the window applies to. |
| `force_run_since_start` | bool | `False` | Reset the watermark for one run. |
| `event_types` | list[str] | `[]` | ER event-type slugs. Empty = no filter. |
| `event_categories` | list[str] | `[]` | ER event-category slugs (AND-combined with types). Empty = no filter. |
| `run_on_schedule` | bool | `False` | Enable scheduled pulling. |

## `PullObservationsConfig` — `pull_observations`

| Field | Type | Default | Notes |
|-------|------|---------|-------|
| `start_datetime` | ISO-8601 string | **required** | First-run window start. Ignored once the watermark advances. |
| `end_datetime` | ISO-8601 string | none | Optional window ceiling; sent every run. |
| `subject_group_ids` | list[str] | `[]` | ER subject-group UUIDs (resolved recursively). Empty = no filter. |
| `subwindow_days` | int | `1` | Backfill slice width in days. |
| `force_run_since_start` | bool | `False` | Reset the watermark for one run. |
| `continue_immediately` | bool | `False` | Self-re-trigger the next backfill chunk via PubSub instead of waiting for the next tick. |
| `run_on_schedule` | bool | `False` | Enable scheduled pulling. |

!!! tip "`run_on_schedule` defaults to off"
    The base `PullActionConfiguration` defaults `run_on_schedule` to `True`, but both pull actions here
    override it to `False` — this integration is most often deployed only as a destination, so scheduled
    pulling is opt-in per connection.

See [State & scheduling](state-and-scheduling.md) for watermark, backfill, and scheduling semantics.
