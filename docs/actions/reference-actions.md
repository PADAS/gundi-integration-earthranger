# Reference actions

Three config-time lookups the Gundi portal calls while an operator is filling out a form — e.g.
populating an "Event Type" dropdown, or the values for a choice field on that event type. They are
**not** scheduled and have no configuration of their own: the caller supplies query params per-call via
`config_overrides`, and each returns a `ReferenceDataResponse` (an `options` list plus a
`cache_ttl_seconds` cache hint) rather than performing any side effect.

| Action | Query model | Purpose |
|--------|-------------|---------|
| `list_event_types` | `ListEventTypesQuery` (no fields) | Every ER event-type slug visible to this integration's credentials, grouped by event category. |
| `list_event_type_fields` | `ListEventTypeFieldsQuery` (`event_type`) | The field keys defined on one event type's schema. |
| `list_event_field_values` | `ListEventFieldValuesQuery` (`event_type`, `field_key`) | The allowed values for one choice field on that event type. |

`action_list_event_types` / `action_list_event_type_fields` / `action_list_event_field_values` —
`app/actions/handlers.py`.

## Why these exist

CMORE's mapping form lets an operator pick an ER event type and one of its fields (e.g. `animal_sex`) to
map onto a CMORE tag field. Those are source-side (ER) values, so the CMORE runner's config form needs to
resolve them from *this* runner — that's `target: "provider"` in the
[reference-data design](https://github.com/PADAS/gundi-integration-cmore/blob/main/docs/rfc-reference-data-portal-support.md).
These three actions are what the portal calls to make that dropdown chain (event type → its fields → a
field's values) work.

## How they're implemented

- `list_event_types` reuses the same `_fetch_event_type_maps` helper `pull_events` uses to resolve
  operator-configured slugs (queries ER's v1 and v2 event-type endpoints and merges the results, falling
  back to the categories endpoint for any category display name neither version's response carried).
  Options are grouped by category display name (or ungrouped, if no display name could be resolved) and
  sorted by group then label.
- `list_event_type_fields` and `list_event_field_values` both fetch ER's pre-rendered event-type schema
  (`/api/v2.0/activity/eventtypes/{event_type}/schema?pre_render=true&s_format=enum` — not modeled by
  `erclient`, so called directly with the client's own auth headers) and parse it with
  `app/actions/er_schema.py`. A choice field's `description` is set to `"choices"` so the caller can tell
  which fields have a fixed value set; free-text fields return no choices for `list_event_field_values`
  (an empty `options` list, not an error — the field is legitimately free text in ER).

## Error semantics

- Unknown `event_type` or `field_key` → the handler raises `ValueError`, which the runner turns into an
  error response (never a leaked config, per the reference-action error carve-out — see
  [Configuration reference](../configuration.md#register_reference_actions)).
- Any other upstream ER failure (e.g. a 5xx) propagates unchanged.

## Registration is gated

These actions register with `"type": "reference"`, a type the Gundi API doesn't accept yet. They stay out
of self-registration entirely until `REGISTER_REFERENCE_ACTIONS` is turned on — see
[Configuration reference](../configuration.md#register_reference_actions).
