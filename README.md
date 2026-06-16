# EarthRanger Action Runner

A [Gundi v2](https://gundiservice.org) integration that pulls **events** and **subject observations** from an
[EarthRanger](https://earthranger.com) site on a schedule and forwards them to Gundi, which routes them onward to any
configured destinations (for example, a C-more instance).

It is **pull-based**: actions are triggered on a schedule via GCP PubSub, fetch data from the ER API, transform it into
the Gundi schema, and send it through the Gundi API.

```
EarthRanger site  →  pull action  →  transform to Gundi schema  →  Gundi API  →  destinations
```

## What it does — the four actions

| Action | What it does |
|--------|--------------|
| `auth` | Validates the configured EarthRanger credentials. |
| `pull_events` | Pulls ER events — and their note/field updates — and sends them to Gundi as events and event updates. |
| `pull_observations` | Pulls tracking observations for the configured ER subject groups and sends them to Gundi as observations. |
| `show_permissions` | Diagnostic. Lists the event categories and subject groups the configured account can access (handy for finding the UUIDs the pull actions need). |

Both pull actions track their own watermark after a successful run, so each scheduled pull only fetches what is new
since the last one. The `start_datetime` / `force_run_since_start` config fields control the first run and one-off
backfills.

## Configuration

The integration is configured per-connection in the [Gundi portal](https://gundiservice.org); each action above has a
configuration form. Connecting a site needs, at minimum, the EarthRanger base URL and credentials (`auth`), plus the
event categories / subject groups to pull. Use `show_permissions` to discover the UUIDs available to the account.

### Integration type slug

The canonical Gundi integration-type slug for this runner is **`earth_ranger`** (with an underscore). It matches
`gundi_core.schemas.v1.DestinationTypes.EarthRanger` and the fixtures across `cdip-routing` and the gundi-integration
repos. Always use `earth_ranger` when setting `INTEGRATION_TYPE_SLUG` in deployed config, running
`python app/register.py --slug ...`, or referencing this integration type elsewhere — the wrong slug breaks
registration. (An `earthranger` type without the underscore existed historically as a registration accident and is
being cleaned up; see [PR #13](https://github.com/PADAS/gundi-integration-earthranger/pull/13).)

## Getting started

- **Run the tests:** `pytest`
- **Run locally:** the runner is a FastAPI service; see [`local/LOCAL_DEVELOPMENT.md`](local/LOCAL_DEVELOPMENT.md) to
  bring it up with Docker Compose against Gundi stage services. The browsable API is then at
  http://localhost:8080/docs.

The integration-specific logic lives in `app/actions/` — `handlers.py` (the four actions above) and
`configurations.py` (their config models). Everything else is Gundi action-runner framework code.

## Documentation

Deeper technical documentation — architecture, the per-event state model, the ER → Gundi transform, and how to add an
action — is coming soon as a dedicated docs site.
