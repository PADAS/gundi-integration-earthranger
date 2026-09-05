# Reference actions

Config-time lookups the Gundi portal calls while an operator is filling out a form — e.g.
populating an "Event Type" dropdown, or the values for a choice field on that event type. They are
**not** scheduled and have no configuration of their own: the caller supplies query params per-call via
`config_overrides`, and each returns a `ReferenceDataResponse` (an `options` list plus a
`cache_ttl_seconds` cache hint) rather than performing any side effect.

| Action | Query model | Purpose |
|--------|-------------|---------|
| `list_event_types` | `ListEventTypesQuery` (no fields) | Every **v2** ER event-type slug visible to this integration's credentials, grouped by event category. Classic v1 event types are not offered — see [Scope](#scope-v2-event-types-only). |
| `list_event_categories` | `ListEventCategoriesQuery` (no fields) | Every ER event-category slug visible to this integration's credentials. |
| `list_event_type_fields` | `ListEventTypeFieldsQuery` (`event_type`) | The field keys defined on one event type's schema. |
| `list_event_field_values` | `ListEventFieldValuesQuery` (`event_type`, `field_key`) | The allowed values for one choice field on that event type. |
| `list_subject_types` | `ListSubjectTypesQuery` (no fields) | The distinct subject **subtypes** observed on the site (from the subjectgroups tree, inactive subjects included), grouped by subject type — with each subject type itself offered first in its group as a fallback option, since destination-side subject mappings match `subject_subtype` first, then `subject_type`. |

`action_list_event_types` / `action_list_event_categories` / `action_list_event_type_fields` /
`action_list_event_field_values` / `action_list_subject_types` — `app/actions/handlers.py`.

## Why these exist

CMORE's mapping form lets an operator pick an ER event type and one of its fields (e.g. `animal_sex`) to
map onto a CMORE tag field. Those are source-side (ER) values, so the CMORE runner's config form needs to
resolve them from *this* runner — that's `target: "provider"` in the
[reference-data design](https://github.com/PADAS/gundi-integration-cmore/blob/main/docs/rfc-reference-data-portal-support.md).
`list_event_types`, `list_event_type_fields` and `list_event_field_values` are what the portal calls
to make that dropdown chain (event type → its fields → a field's values) work.
`list_subject_types` serves the same form's subject-mapping lists (subject type → affiliation /
classification): it offers the subtypes actually flowing through Gundi observations, so operators
pick real values instead of guessing slugs.

## Used by this runner's own form too

`pull_events`' **Event Types** and **Event Categories** config fields carry
`gundi:reference` annotations with `target: "self"` — in a supporting portal,
each list item renders as a live dropdown backed by `list_event_types` /
`list_event_categories` on this very integration, so operators pick slugs
instead of copying them from `show_permissions` output. Older portals ignore
the annotation and keep plain text inputs.

## Scope: v2 event types only

`list_event_types` offers only **v2**-sourced event types. ER's v2 pre-rendered schema endpoint (the one
`list_event_type_fields` / `list_event_field_values` depend on) 404s for classic v1 event types — offering
a v1 slug here would dead-end the dropdown cascade one step later, with a "not supported" error where an
operator would expect a list of fields. Rather than offer a type that can't be followed, `list_event_types`
excludes v1 slugs entirely.

This doesn't block an operator from referencing a v1 event type in a mapping — the `gundi:reference`
widget contract degrades to a plain free-text input whenever the reference fetch has nothing to offer (or
fails), so a v1 `event_type` / `event_details` key can still be typed in by hand; it just doesn't get
autocomplete. No further integration-side work is planned for v1 — this is the intended long-term shape,
not an interim gap to close later.

## How they're implemented

- `list_event_types` reuses the same `_fetch_event_type_maps` helper `pull_events` uses to resolve
  operator-configured slugs (queries ER's v1 and v2 event-type endpoints and merges the results), but
  narrows the result to `EventTypeMaps.v2_slugs` before building options. Category display names can come
  from either version's response; unlike `_fetch_event_type_maps`'s own categories-endpoint fallback (which
  only fires when v1 didn't already resolve category UUIDs — tuned for `pull_events`' filter-resolution
  need), `list_event_types` always fetches `get_event_categories` itself, so a v2-only category's display
  name resolves even when a v1 type elsewhere already satisfied that fallback's trigger condition. Options
  are grouped by category display name (or ungrouped, if no display name could be resolved) and sorted by
  group then label.
- `list_event_type_fields` and `list_event_field_values` both fetch ER's pre-rendered event-type schema
  (`/api/v2.0/activity/eventtypes/{event_type}/schema?pre_render=true&s_format=enum` — not modeled by
  `erclient`, so called directly with the client's own auth headers) and parse it with
  `app/actions/er_schema.py`. A choice field's `description` is set to `"choices"` so the caller can tell
  which fields have a fixed value set; free-text fields return no choices for `list_event_field_values`
  (an empty `options` list, not an error — the field is legitimately free text in ER).

## Multiple providers on one destination

These actions answer for **one** ER integration — the one whose credentials the
runner holds. When a destination (e.g. a CMORE integration) is connected to
*several* ER providers, it's the **portal** that fans a dropdown's query out to
each provider separately and merges the results: options are unioned and
deduped by value, and a provider that errors for a given query (say, an event
type it doesn't define) is skipped as long as at least one provider answers.
Nothing in this runner needs to know about the other providers — each instance
only ever reports its own site's vocabulary.

Practical upshot for operators: a shared destination's "Event Type" dropdown
shows every connected site's event types; type-specific field/value lookups
come from whichever site(s) define that type.

## Error semantics

- Unknown `event_type` or `field_key`, an empty Site URL, or missing auth settings → the handler raises
  `IntegrationConfigurationError`. The runner reports it as `Invalid configuration — <message>`; on the
  ephemeral (draft-integration) path it is forwarded to the portal wizard with a 422, where every other
  connector message is redacted. The messages describe the problem without echoing the submitted value. A
  classic v1 `event_type` passed to `list_event_type_fields` / `list_event_field_values` surfaces the same way
  (ER's 404), with wording that calls out the v1/v2 distinction rather than implying the slug is simply unknown.
- EarthRanger client failures (bad credentials, permission denied, rate limit, 5xx, unreachable) are translated
  into the runner's classified errors with the source status, so the wizard sees e.g.
  `Authentication failed (HTTP 401)` and a 401 rather than a bare 500.
- Any other upstream ER failure propagates unchanged and is classified by its status.

## Registration

These actions register with `"type": "reference"` whenever the runner self-registers. Earlier builds gated
this behind `REGISTER_REFERENCE_ACTIONS` until the Gundi API accepted the type; that gate is gone — see
[Configuration reference](../configuration.md#reference-action-registration).
