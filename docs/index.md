# EarthRanger Action Runner

A [Gundi v2](https://gundiservice.org) integration that pulls **events** and **subject observations**
from an [EarthRanger](https://earthranger.com) (ER) site on a schedule and forwards them to Gundi,
which routes them onward to any configured destinations (for example, a C-more instance).

It is **pull-based**: actions are triggered on a schedule via GCP PubSub, fetch data from the ER API,
transform it into the Gundi schema, and send it through the Gundi API.

```
EarthRanger site  →  pull action  →  transform to Gundi schema  →  Gundi API  →  destinations
```

## The four actions

| Action | What it does |
|--------|--------------|
| [`auth`](actions/auth.md) | Validates the configured EarthRanger credentials (token or username/password). |
| [`pull_events`](actions/pull-events.md) | Pulls ER events — plus their note and field updates — and sends them to Gundi as events and event updates. |
| [`pull_observations`](actions/pull-observations.md) | Pulls tracking observations for the configured ER subject groups and sends them to Gundi as observations. |
| [`show_permissions`](actions/show-permissions.md) | Diagnostic. Lists the event categories and subject groups the configured account can access — and the UUIDs the pull actions need. |

## How to read these docs

- **[Architecture](architecture.md)** — the runtime: how a PubSub message becomes an action execution, config caching, scheduling, self-registration.
- **[Actions](actions/index.md)** — what each action does and every config field it accepts.
- **[Data flow](data-flow.md)** — how ER events and observations are transformed into the Gundi schema, including the note/field → event-update path.
- **[State & scheduling](state-and-scheduling.md)** — per-event state, watermarks, and the resumable observation backfill.
- **[Configuration reference](configuration.md)** — every field of every config model in one table.
- **[Local development](local-development.md)** — run the service locally and run the tests.
- **[Contributing](contributing.md)** — how to add a new action.

## Integration type slug

The canonical Gundi integration-type slug for this runner is **`earth_ranger`** (with an underscore).
It matches `gundi_core.schemas.v1.DestinationTypes.EarthRanger` and the fixtures across `cdip-routing`
and the gundi-integration repos. Always use `earth_ranger` when setting `INTEGRATION_TYPE_SLUG`, running
`python app/register.py --slug …`, or referencing this integration type elsewhere — the wrong slug breaks
registration.

!!! note
    An `earthranger` type without the underscore existed historically as a registration accident and is
    being cleaned up. See [PR #13](https://github.com/PADAS/gundi-integration-earthranger/pull/13).
