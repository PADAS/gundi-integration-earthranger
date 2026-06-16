# Contributing

## Where the code lives

| Path | Purpose |
|------|---------|
| `app/actions/handlers.py` | The `action_*` functions — all EarthRanger-specific logic. |
| `app/actions/configurations.py` | The Pydantic config model for each action. |
| `app/actions/tests/` | Action tests (and shared fixtures in `conftest.py`). |
| `app/services/` | Gundi action-runner framework: dispatch, config cache, state, send helpers. |

Everything outside `app/actions/` is framework code shared with the
[action-runner template](https://github.com/PADAS/gundi-integration-action-runner) — prefer not to fork it.

## Adding a new action

1. **Define a config model** in `app/actions/configurations.py`. Subclass the appropriate base:
   `AuthActionConfiguration`, `PullActionConfiguration`, `PushActionConfiguration`, or
   `GenericActionConfiguration`. Use `FieldWithUIOptions(...)` + `UIOptions(...)` for portal field rendering
   and set `ui_global_options.order` to control field order.
2. **Write the handler** in `app/actions/handlers.py` named `action_<id>`, annotated so it's
   auto-discovered:
   ```python
   @activity_logger()
   async def action_my_thing(integration: Integration, action_config: MyThingConfig):
       ...
   ```
   The runner reads the `action_config` annotation to bind the config model; push actions also take
   `data` and `metadata` parameters.
3. **Add `@crontab_schedule("…")`** if it should run periodically (pull actions). Remember both existing
   pull actions default `run_on_schedule` to `False`.
4. **Talk to EarthRanger** through the ER client; **send to Gundi** through `app/services/gundi.py`
   (`send_events_to_gundi`, `send_observations_to_gundi`, `update_event_in_gundi`). Keep network calls
   resilient (the existing send helpers retry with backoff).
5. **Persist incremental state** through `IntegrationStateManager` if the action is incremental — see
   [State & scheduling](state-and-scheduling.md) for the watermark/cursor patterns.
6. **Test it.** Add tests under `app/actions/tests/`, mocking the ER client and Gundi senders (follow the
   existing tests as a pattern). Run `pytest`.

## Conventions

- **Pydantic** for all data structures (config models, DTOs) — not dataclasses.
- **Emit one event-update per logical change** (the GUNDI-5386 contract) so each surfaces cleanly
  downstream. See [Data flow](data-flow.md#notes-and-field-changes-event-updates).
- **Advance watermarks only after a unit of work is durably forwarded**, so a failure resumes rather than
  skips.

## Updating these docs

Docs are MkDocs Material under `docs/`. Edit the relevant page, preview with `mkdocs serve`, and confirm
`mkdocs build --strict` passes (that's what CI runs). Merging to `main` republishes the site.
