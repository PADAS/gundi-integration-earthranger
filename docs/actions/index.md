# Actions

The runner implements seven actions in `app/actions/handlers.py` as `action_*` functions
with a matching config model in `app/actions/configurations.py`. Four self-register
unconditionally; the three reference actions register only when
[`REGISTER_REFERENCE_ACTIONS`](../configuration.md#register_reference_actions) is enabled.

| Action | Type | Purpose |
|--------|------|---------|
| [`auth`](auth.md) | auth | Validate ER credentials. |
| [`pull_events`](pull-events.md) | pull | ER events + note/field updates → Gundi events & event updates. |
| [`pull_observations`](pull-observations.md) | pull | ER subject tracking → Gundi observations. |
| [`show_permissions`](show-permissions.md) | generic / diagnostic | Show what the account can access and the UUIDs the pull actions need. |
| [`list_event_types`, `list_event_type_fields`, `list_event_field_values`](reference-actions.md) | reference | Config-time lookups for the Gundi portal's mapping forms. Disabled by default — see [`REGISTER_REFERENCE_ACTIONS`](../configuration.md#register_reference_actions). |

**Typical setup order:** run `auth` to confirm credentials → run `show_permissions` to discover event-type
/ category slugs and subject-group UUIDs → configure and enable `pull_events` / `pull_observations` with
those values. The reference actions require no setup of their own — the portal calls them directly once
`REGISTER_REFERENCE_ACTIONS` is enabled.

See the [Configuration reference](../configuration.md) for every field of every model in one place.
