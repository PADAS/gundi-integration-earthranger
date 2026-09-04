# Actions

The runner implements seven actions in `app/actions/handlers.py` as `action_*` functions
with a matching config model in `app/actions/configurations.py`. All of them self-register
unconditionally; the reference actions register with `"type": "reference"`.

| Action | Type | Purpose |
|--------|------|---------|
| [`auth`](auth.md) | auth | Validate ER credentials. |
| [`pull_events`](pull-events.md) | pull | ER events + note/field updates → Gundi events & event updates. |
| [`pull_observations`](pull-observations.md) | pull | ER subject tracking → Gundi observations. |
| [`show_permissions`](show-permissions.md) | generic / diagnostic | Show what the account can access and the UUIDs the pull actions need. |
| [`list_event_types`, `list_event_type_fields`, `list_event_field_values`](reference-actions.md) | reference | Config-time lookups for the Gundi portal's mapping forms. |

**Typical setup order:** run `auth` to confirm credentials → run `show_permissions` to discover event-type
/ category slugs and subject-group UUIDs → configure and enable `pull_events` / `pull_observations` with
those values. The reference actions require no setup of their own — the portal calls them directly.

See the [Configuration reference](../configuration.md) for every field of every model in one place.
