# Architecture

The runner is a [FastAPI](https://fastapi.tiangolo.com/) service built on the shared
[Gundi action-runner template](https://github.com/PADAS/gundi-integration-action-runner). Almost all
EarthRanger-specific logic lives in `app/actions/` — `handlers.py` (the action functions) and
`configurations.py` (their config models). Everything else is framework code.

## Request flow

Actions are triggered by GCP PubSub messages delivered to the service over HTTP.

```
GCP PubSub message
   → POST /                       (app/main.py)
   → base64-decode the message, parse JSON
   → execute_action(integration_id, action_id)   (app/services/action_runner.py)
   → load integration + action config             (config_manager, Redis-cached)
   → look up the handler by action_id             (app/actions/__init__.py)
   → parse/validate the config model
   → run the handler with a timeout
   → @activity_logger publishes start/complete/error events to PubSub
```

### HTTP endpoints (`app/main.py`)

| Endpoint | Purpose |
|----------|---------|
| `GET /` | Health check (`{"status": "healthy"}`). |
| `POST /` | Primary entry point. Decodes a base64 PubSub message and runs the named action. |
| `POST /push-data` | Push ingestion: runs an action selected by the payload's data type, with `destination_id` taken from the message attributes. |
| `POST /v1/actions/execute` | Synchronous execution endpoint (`app/routers/actions.py`) used for manual/triggered runs. |

Whether a `POST /` message runs inline or as a background task is governed by
`PROCESS_PUBSUB_MESSAGES_IN_BACKGROUND` (default off / synchronous).

## Action dispatch

Handlers are discovered automatically at import time. `app/actions/__init__.py` calls
`discover_actions(module_name="app.actions.handlers", prefix="action_")`, which inspects every
function named `action_*` and reads its type annotations to build a registry:

```
action_handlers = { action_id: (handler_func, ConfigModel, DataModel), ... }
```

`app/services/action_runner.py::execute_action()` then:

1. Loads the integration via `config_manager.get_integration_details()`.
2. Looks up `action_handlers[action_id]` (or, for push data, matches by data-model name).
3. Fetches and validates the action's config (`config_model.parse_obj(...)`, plus any `config_overrides`).
4. Decides whether the run is **manual** or **scheduled** — scheduled pulls that are missing config,
   fail validation, or have `run_on_schedule` off are skipped quietly rather than erroring.
5. Runs the handler with a timeout of `MAX_ACTION_EXECUTION_TIME` (default 540 s / 9 min; a 504 is
   returned on timeout).

## Configuration cache

`app/services/config_manager.py` fetches integration and action config from the Gundi API and caches it
in Redis (`REDIS_CONFIGS_DB`, default DB 1) under keys like `integration.{id}` and
`integrationconfig.{id}.{action_id}`. Fetches retry with exponential backoff on HTTP errors.

State (watermarks, per-event records, backfill cursors) is stored separately by
`IntegrationStateManager` in `REDIS_STATE_DB` (default DB 0). See
[State & scheduling](state-and-scheduling.md).

## Scheduling

Pull actions can run on a schedule. A schedule is attached either with the `@crontab_schedule("…")`
decorator on the handler or via `python app/register.py --schedule "action_id:<crontab>"`. Crontab specs
support the standard five fields plus an optional timezone offset
(`app/services/action_scheduler.py`).

Note that both pull actions default `run_on_schedule` to **`False`** — this integration is most often
deployed only as a *destination*, so scheduled pulling is opt-in per connection.

## Self-registration

On startup, if `REGISTER_ON_START` is set, the service registers its integration type with Gundi
(`app/services/self_registration.py`), publishing each action's JSON schema and marking pull actions as
periodic. The same logic is runnable from the CLI:

```bash
python app/register.py --slug earth_ranger --service-url <public-url>
```

## Activity logging

The `@activity_logger()` decorator on an action publishes **start**, **complete**, and **error** events to
the `INTEGRATION_EVENTS_TOPIC` PubSub topic; these surface in the Gundi portal's activity feed. Handlers
also call `log_action_activity(...)` to emit custom INFO/WARNING/ERROR entries (used, for example, when a
pull is skipped because no configured event types resolved).

## Key environment variables

| Variable | Default | Purpose |
|----------|---------|---------|
| `GUNDI_API_BASE_URL` | — | Gundi platform API endpoint. |
| `INTEGRATION_TYPE_SLUG` | — | This integration type's slug — **`earth_ranger`**. |
| `INTEGRATION_SERVICE_URL` | — | Public URL of this service (for self-registration). |
| `REGISTER_ON_START` | `False` | Auto-register the integration type on startup. |
| `REDIS_HOST` / `REDIS_PORT` | `localhost` / `6379` | Redis host for config + state. |
| `REDIS_STATE_DB` / `REDIS_CONFIGS_DB` | `0` / `1` | Redis DBs for state and config cache. |
| `INTEGRATION_EVENTS_TOPIC` | `integration-events` | PubSub topic for activity/error events. |
| `INTEGRATION_COMMANDS_TOPIC` | `{slug}-actions-topic` | PubSub topic used to self-trigger the next backfill chunk. |
| `MAX_ACTION_EXECUTION_TIME` | `540` | Handler timeout, seconds. |
| `PROCESS_PUBSUB_MESSAGES_IN_BACKGROUND` | `False` | Process `POST /` messages as background tasks. |
