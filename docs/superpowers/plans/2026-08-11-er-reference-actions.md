# ER Provider Reference Actions Implementation Plan (Phase 2)

> **For agentic workers:** execute task-by-task with a review after each task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** The EarthRanger runner exposes three provider-side reference actions — `list_event_types`, `list_event_type_fields(event_type)`, `list_event_field_values(event_type, field_key)` — per the reference-data RFC (`gundi-integration-cmore/docs/rfc-reference-data-portal-support.md`, Phase 2) and design doc §4 (`gundi-integration-cmore/docs/superpowers/specs/2026-07-31-reference-data-config-ui-design.md`), gated behind a default-off `REGISTER_REFERENCE_ACTIONS` env flag exactly like CMORE's Phase 0.

**Architecture:** Two parts. (1) A verbatim port of CMORE Phase 0's framework support — the two repos' template files are byte-identical apart from that diff: `ReferenceActionConfiguration`/`ReferenceOption`/`ReferenceDataResponse` marker+envelope in `app/actions/core.py`, `REFERENCE` in `app/services/core.py`'s `ActionTypeEnum`, the registration gate + type branch in `app/services/self_registration.py`, and the 404-exemption + secret-scrubbing carve-outs in `app/services/action_runner.py`, plus the settings flag. (2) ER-specific handlers: `list_event_types` uses `AsyncERClient.get_event_types`/`get_event_categories` (the `_fetch_event_type_maps` pattern already in handlers.py); the two schema-driven actions fetch ER's pre-rendered v2 schema endpoint (`/api/v2.0/activity/eventtypes/{event_type}/schema?pre_render=true&s_format=enum` — not exposed by erclient, so a raw httpx GET authenticated via `await er_client.auth_headers()`) and parse it with a ported copy of CMORE's `parse_er_event_schema`.

**Tech Stack:** Python 3.10, FastAPI action-runner template, erclient (AsyncERClient), httpx, pytest (+pytest-asyncio, pytest-mock). Test command from repo root: `.venv/bin/pytest` (baseline to record at start).

## Global Constraints

- The framework port (Task 1) must be a faithful copy of CMORE's diff — do not redesign. Source files: `gundi-integration-cmore/app/actions/core.py:64-87` (+`List` import), `app/services/core.py:9`, `app/services/self_registration.py:15,21,57-62,66-67`, `app/services/action_runner.py:19,242-244,248,294-304,323,329`, `app/settings/integration.py:14-19`.
- `REGISTER_REFERENCE_ACTIONS` defaults **False** (the Gundi API PR PADAS/cdip#461 is not merged/deployed yet; registering `"reference"` before it lands would 400 the whole registration).
- Reference handlers are stateless: read only the integration's `auth` config (via the existing `get_authentication_config(integration)` at `app/actions/handlers.py:1119-1129`), take query params from `config_overrides` via their Pydantic query model, return `ReferenceDataResponse(...).dict()`.
- Handler errors: unknown event_type/field → raise `ValueError` (matches CMORE; the runner scrubs configs from reference-action error payloads per the ported carve-out).
- Options envelope semantics: `value` is what gets stored in downstream config (event-type **slug**, field **key**, choice **value**); `label` is the human display name.
- Follow ER handler conventions: `AsyncERClient` built per-handler from `urlparse(integration.base_url)` exactly like `action_show_permissions` (`handlers.py:308-316`); `DEFAULT_CONNECT_TIMEOUT_SECONDS` for connect timeout.

## Key verified facts

- ER's `self_registration.py`/`action_runner.py`/`actions/core.py`/`services/core.py` are byte-identical to CMORE's minus the Phase 0 diff (verified file-by-file). `app/settings/integration.py` differs structurally — add only the flag lines.
- erclient: `get_event_types(include_inactive=False, include_schema=False, version=...)` (client.py:1610; v1 path `activity/events/eventtypes`, v2 `activity/eventtypes`), `get_event_categories()` (:1625), `get_event_type(name, version, include_schema)` (:1648). `VERSION_1_0`/`VERSION_2_0` already imported in ER handlers.py:13. The async client has `auth_headers()` (used by `get_file`) — returns the Authorization header dict, handling both token and username/password auth.
- The pre-rendered schema endpoint (inlines each choice field's enum + `x-enumExtra` so no separate choices fetch is needed) is only reachable via raw httpx — mirror `gundi-integration-cmore/app/datasource/cli.py:598-608`.
- Parser to port: `gundi-integration-cmore/app/datasource/er_schema.py` — `ERField` (key/title/choices/choices_ref/choice_list), `ERChoice(value, display)`, `parse_er_event_schema(raw) -> List[ERField]`, `_inline_choices`. Port as `app/actions/er_schema.py` in the ER repo (module docstring crediting the origin); drop the parts only the CLI needs (`parse_choices_json`, `choices_ref` external resolution) ONLY if the pre-rendered endpoint truly inlines everything — verify against the fixture first and keep them if any fixture field uses refs.
- Test fixture source: `gundi-integration-cmore/docs/rhino_carcass_schema_from_api.json` is a REAL captured response from the pre-rendered endpoint — copy it into the ER repo's test fixtures (`app/actions/tests/fixtures/`).
- CMORE tests to port/adapt: `app/services/tests/test_self_registration.py:766-829` (gate off/on + type assertion), `app/services/tests/test_action_runner.py:730-830` (zero-param no-404, error-no-config-leak, ordinary-action-still-404s). ER's equivalents live in `app/services/tests/` with the same fixtures (verify names; the repos' conftest.py for services tests should match).

## File structure

| File | Change |
|---|---|
| `app/actions/core.py` | Port marker + envelope models |
| `app/services/core.py` | Port `REFERENCE` enum member |
| `app/services/self_registration.py` | Port gate + type branch |
| `app/services/action_runner.py` | Port 404-exemption + secret-scrub carve-outs |
| `app/settings/integration.py` | Add `REGISTER_REFERENCE_ACTIONS = env.bool(..., False)` (+env import if absent) |
| `app/actions/er_schema.py` | Create — ported schema parser |
| `app/actions/configurations.py` | Add 3 query models |
| `app/actions/handlers.py` | Add 3 handlers + schema-fetch helper |
| `app/actions/tests/fixtures/rhino_carcass_schema_from_api.json` | Copy fixture |
| `app/actions/tests/test_reference_actions.py` | Create — handler tests |
| `app/services/tests/test_self_registration.py`, `test_action_runner.py` | Port reference-action test groups |
| `docs/actions/` + `docs/configuration.md` | Document the actions + flag |

---

### Task 1: framework port (verbatim) + registration/runner tests

- [ ] Copy each CMORE hunk listed in Global Constraints into the ER twins — after each file, `diff` it against the CMORE version to confirm the only remaining differences are pre-existing/unrelated ones (state.py-style divergences are listed in the scout notes; these four files should end up byte-identical or explainably close).
- [ ] `app/settings/integration.py`: append the flag block (mirroring CMORE lines 14-19, including the comment about staying off until the platform supports the type). Verify `from app import settings; settings.REGISTER_REFERENCE_ACTIONS` imports.
- [ ] Port the CMORE test groups (self_registration gate off/on; action_runner zero-param no-404 / error-no-config-leak / ordinary-action-404 guard), adapting fixture names to ER's test files. TDD order: tests first (fail on missing imports), then the port.
- [ ] Full suite green. Commit: `feat: framework support for reference actions (ported from CMORE Phase 0)`.

### Task 2: ER schema module + fixture

- [ ] Copy `rhino_carcass_schema_from_api.json` into `app/actions/tests/fixtures/`.
- [ ] Create `app/actions/er_schema.py` ported from CMORE's `app/datasource/er_schema.py` (keep `ERField`/`ERChoice`/`parse_er_event_schema`/`_inline_choices`; decide on ref-resolution parts per the fixture). Add:

```python
async def fetch_prerendered_event_schema(er_client, base_url: str, event_type: str) -> dict:
    """GET the v2 pre-rendered event-type schema (pre_render + s_format=enum inline
    each choice field's values, so no separate choices fetch is needed). erclient
    doesn't model this /schema sub-resource, so we call it directly with the
    client's own auth headers (works for both token and username/password auth)."""
    url_parse = urlparse(base_url)
    url = f"{url_parse.scheme}://{url_parse.hostname}/api/v2.0/activity/eventtypes/{quote(event_type, safe='')}/schema"
    headers = await er_client.auth_headers()
    async with httpx.AsyncClient(headers=headers, timeout=30.0) as http:
        resp = await http.get(url, params={"pre_render": "true", "s_format": "enum"})
        resp.raise_for_status()
        return resp.json()
```

- [ ] Unit tests (`test_reference_actions.py` or a dedicated `test_er_schema.py`): `parse_er_event_schema` against the fixture (assert known rhino_carcass fields + inlined choices); `fetch_prerendered_event_schema` with mocked `auth_headers` + `respx`/mocked httpx asserting URL, params, and auth header propagation; a 404 from ER surfaces as `httpx.HTTPStatusError`.
- [ ] Full suite green. Commit: `feat: ER pre-rendered event-schema fetch + parser for reference actions`.

### Task 3: query models, handlers, docs

- [ ] `app/actions/configurations.py` — three models (mirror CMORE's style; `ReferenceActionConfiguration` import from `.core`):

```python
class ListEventTypesQuery(ReferenceActionConfiguration):
    """List the ER event types visible to this integration's credentials."""


class ListEventTypeFieldsQuery(ReferenceActionConfiguration):
    event_type: str


class ListEventFieldValuesQuery(ReferenceActionConfiguration):
    event_type: str
    field_key: str
```

- [ ] `app/actions/handlers.py` — three handlers using `get_authentication_config` + the standard `AsyncERClient` construction:
  - `action_list_event_types`: reuse the `_fetch_event_type_maps` approach (v2 first, v1 fallback, categories for grouping): options = `ReferenceOption(value=<slug>, label=<display>, group=<category display or None>)`; sort by group then label; `truncated=False`.
  - `action_list_event_type_fields`: `fetch_prerendered_event_schema` → `parse_er_event_schema` → options = `ReferenceOption(value=field.key, label=field.title or field.key, description=("choices" if field.is_enum else None))`. Unknown event_type (HTTP 404 from ER) → `raise ValueError(f"Event type '{...}' not found in EarthRanger.")`.
  - `action_list_event_field_values`: same fetch/parse, locate the field by key (`ValueError` if absent), options = its choices as `ReferenceOption(value=choice.value, label=choice.display)`; non-enum field → empty options (matches CMORE's `list_field_options` non-lookup behavior).
- [ ] Handler tests in `test_reference_actions.py` (mock AsyncERClient + the fetch helper; use the fixture): happy paths for all three; grouping in `list_event_types`; unknown event_type → ValueError; unknown field_key → ValueError; non-enum field → empty options; upstream HTTP 500 propagates.
- [ ] Docs: add a "Reference actions" section to the actions docs + the `REGISTER_REFERENCE_ACTIONS` row in `docs/configuration.md` (mirroring the CMORE wording; note default-off until PADAS/cdip#461 is deployed).
- [ ] Full suite green. Commit: `feat: reference actions — list_event_types, list_event_type_fields, list_event_field_values`.

### Task 4: rollout checklist (manual)

- [ ] Keep `REGISTER_REFERENCE_ACTIONS=false` everywhere until PADAS/cdip#461 is merged AND deployed to the target environment.
- [ ] Then flip it on the dev ER runner, confirm registration succeeds and the three actions appear with `"type": "reference"`, and smoke-test via `POST /v1/actions/execute` against dev ER.
- [ ] Phase 2 completion (separate change, CMORE repo): flip CMORE's `event_type`/`event_details_key` annotations to `target: "provider"` once the portal supports provider-target resolution.

---

## Self-Review

- RFC Phase 2 asks → Tasks 2-3 (the three actions); parity with Phase 0's platform-safety behavior (gate, 404-exemption, no-secret-leak) → Task 1; rollout ordering vs cdip#461 → Task 4 and the default-off constraint.
- Names consistent: `fetch_prerendered_event_schema`/`parse_er_event_schema` (T2) consumed in T3; query models' fields (`event_type`, `field_key`) match the design doc §4 signature `list_event_field_values(event_type, field_key)`.
- Deliberate scope cuts: no external choices-ref resolution unless the fixture demands it; no erclient upstream PR (raw httpx documented as the reason); `target:"provider"` annotation flips stay out (separate CMORE change).
