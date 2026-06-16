# Actions

The runner exposes four actions, all implemented in `app/actions/handlers.py` as `action_*` functions
with a matching config model in `app/actions/configurations.py`.

| Action | Type | Purpose |
|--------|------|---------|
| [`auth`](auth.md) | auth | Validate ER credentials. |
| [`pull_events`](pull-events.md) | pull | ER events + note/field updates → Gundi events & event updates. |
| [`pull_observations`](pull-observations.md) | pull | ER subject tracking → Gundi observations. |
| [`show_permissions`](show-permissions.md) | generic / diagnostic | Show what the account can access and the UUIDs the pull actions need. |

**Typical setup order:** run `auth` to confirm credentials → run `show_permissions` to discover event-type
/ category slugs and subject-group UUIDs → configure and enable `pull_events` / `pull_observations` with
those values.

See the [Configuration reference](../configuration.md) for every field of every model in one place.
