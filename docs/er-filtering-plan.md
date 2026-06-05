# Plan: Filtered ER pull actions for CMORE (and future destinations)

## Goal

Extend `gundi-integration-earthranger` so that `action_pull_events` can be narrowed by event type / category, and `action_pull_observations` can be narrowed by subject group(s). This is the last piece needed to complete the end-to-end CMORE flow: ER (filtered) → Gundi sensors API → cdip-routing → cmore-push-data-topic → cmore action runner → CMORE.

We build on the existing repo rather than replacing it. The existing auth, state-management, scheduling, and `action_show_permissions` plumbing all stay.

## Decisions locked in

| Question | Decision | Note |
|---|---|---|
| Filter value shape | **ER `value` slugs** for event types, **UUIDs** for subject groups | Closer to the API; no name-resolution layer to maintain. Portal will eventually populate these as dynamic choices (separate ticket). |
| Empty filter list semantics | **No constraint** (additive filter) | Consistent with sibling Gundi pull actions. Misconfig is caught at *action start* with an `ActivityLog` ERROR + zero-result return — same channel operators already watch. No new config field; no save-time validator. |
| Observation filter strategy | **Single-window pull + in-process filter by source_id** | ER observation payloads carry `source`, not `subject_id`. Two pre-loop calls (`get_subjectgroups`, `get_source_assignments`) resolve the configured group UUIDs to a source-id set, then one paginated `get_observations()` per run with an in-process filter. No fan-out, no semaphore. |
| Scope of this round | **Filters only** | Defer event attachments and gundi-client-v2 3.x bump to follow-ups. |

## Changes

### 1. `app/actions/configurations.py`

Add filter fields. Both default to empty lists; empty means "no constraint" (consistent with the rest of the codebase). No `require_filters` field — misconfigs are surfaced at runtime via the activity log instead of at save time (simpler, same operator feedback channel).

```python
from pydantic import Field


class PullEventsConfig(PullActionConfiguration):
    start_datetime: str
    end_datetime: Optional[str] = None
    force_run_since_start: bool = False
    event_types: List[str] = Field(
        default_factory=list,
        title="Event Types",
        description=(
            "List of ER event-type slugs to pull, e.g. ['wildlife_sighting_rep', 'poacher_sighting_rep']. "
            "Run the 'show_permissions' action to see the slugs available for this account. "
            "Combined with Event Categories using ER's AND semantics. "
            "An empty list applies no event-type constraint."
        ),
    )
    event_categories: List[str] = Field(
        default_factory=list,
        title="Event Categories",
        description=(
            "List of ER event-category slugs, e.g. ['wildlife', 'monitoring']. "
            "ER applies type and category filters with AND semantics. "
            "An empty list applies no category constraint."
        ),
    )

    ui_global_options: GlobalUISchemaOptions = GlobalUISchemaOptions(
        order=["start_datetime", "end_datetime", "event_types", "event_categories", "force_run_since_start"],
    )


class PullObservationsConfig(PullActionConfiguration):
    start_datetime: str
    end_datetime: Optional[str] = None
    force_run_since_start: bool = False
    subject_group_ids: List[str] = Field(
        default_factory=list,
        title="Subject Group IDs",
        description=(
            "List of ER subject-group UUIDs whose members' observations should be included. "
            "Picking a parent group includes its sub-groups' subjects (resolved recursively). "
            "Run the 'show_permissions' action to find UUIDs available to this account. "
            "An empty list applies no group constraint."
        ),
    )

    ui_global_options: GlobalUISchemaOptions = GlobalUISchemaOptions(
        order=["start_datetime", "end_datetime", "subject_group_ids", "force_run_since_start"],
    )
```

### 2. `app/actions/handlers.py` — `action_pull_events`

Inject `event_types` / `event_categories` into the existing ER filter dict. Rename the local `filter` to `event_filter` to avoid shadowing the builtin while we're touching it.

```python
event_filter = {"date_range": {"lower": start_datetime}}
if pull_config.end_datetime:
    event_filter["date_range"]["upper"] = pull_config.end_datetime
if pull_config.event_types:
    event_filter["event_type"] = pull_config.event_types
if pull_config.event_categories:
    event_filter["event_category"] = pull_config.event_categories
```

ER applies both filters with AND semantics server-side. Nothing else in the handler changes.

### 3. `app/actions/handlers.py` — `action_pull_observations`

Single-window pull + in-process filter by `source`. No fan-out.

Note on volume: the handler still streams every observation in the date window from ER and filters in-process — fine for the configured groups' typical scale, but a large account configured with a small filter will see most of the bytes thrown away. A future improvement is to push the filter server-side (if ER's observation endpoint grows a multi-subject filter) — flagged as a follow-up.

```python
async def action_pull_observations(integration, action_config):
    integration_id = str(integration.id)
    pull_config = action_config
    ...

    async with er_client as earth_ranger:
        source_id_set = await _resolve_source_ids(
            earth_ranger,
            group_ids=pull_config.subject_group_ids,
        )
        if pull_config.subject_group_ids and not source_id_set:
            await log_activity(
                integration_id=integration_id,
                action_id="pull_observations",
                level=LogLevel.ERROR,
                title="Configured subject groups resolved to zero active sources",
                data={"subject_group_ids": pull_config.subject_group_ids},
            )
            # State is intentionally NOT updated — preserves the previous watermark
            # so a fix on the operator side can re-pull the window.
            return {"observations_extracted": 0, "filter_active": True, "sources_resolved": 0}

        filter_active = bool(source_id_set)
        total_observations = 0
        async for batch in earth_ranger.get_observations(
            start=start_datetime,
            end=pull_config.end_datetime,
            batch_size=BATCH_SIZE,
        ):
            if filter_active:
                batch = [o for o in batch if str(o.get("source", "")) in source_id_set]
            transformed = transform_observations_to_gundi_schema(observations=batch)
            if not transformed:
                continue
            await send_observations_to_gundi(observations=transformed, integration_id=integration_id)
            total_observations += len(transformed)

    # State is advanced normally on success.
    await state_manager.set_state(
        integration_id=integration_id,
        action_id="pull_observations",
        state={"last_execution": execution_timestamp},
    )
    return {
        "observations_extracted": total_observations,
        "filter_active": filter_active,
        "sources_resolved": len(source_id_set) if filter_active else None,
    }
```

`filter_active` disambiguates "0 because filter resolved to zero sources" from "0 because no filter configured" in the activity log payload.

New helper. ER observation payloads carry `source` (a source UUID), not `subject_id`, so resolution is two-step: groups → subjects → sources. The `assigned_range` field on a SubjectSource is intentionally ignored — `get_source_assignments` already filters to *current* assignments, so under-pull (a source reassigned to a subject outside the configured group no longer appears in the set) is the only failure mode, never over-pull / data bleed.

```python
async def _resolve_source_ids(er_client, group_ids: List[str]) -> set:
    """Resolve subject-group UUIDs to a set of source UUIDs.

    Walks ER's subjectgroup tree recursively (flat=False). When a matched UUID
    is found, every descendant subject is included. This matches the operator's
    intuition from `action_show_permissions`, which rolls children UP into every
    ancestor's display — so picking any displayed UUID resolves to the same set
    of subjects at runtime, regardless of which ancestor level was chosen.

    Then queries `subjectsources` in a single batch call to find the sources
    currently assigned to those subjects. Returns source UUIDs as strings.

    `er_client.get_subjectgroups(flat=False)` returns a list (not an async
    iterator) — the full tree fits in one response.
    """
    if not group_ids:
        return set()

    wanted = set(group_ids)
    groups = await er_client.get_subjectgroups(flat=False)
    subject_ids: set = set()

    def walk(group, inherited: bool = False):
        matched = inherited or group.get("id") in wanted
        if matched:
            for subject in group.get("subjects", []):
                subject_ids.add(subject["id"])
        for sub in group.get("subgroups", []):
            walk(sub, inherited=matched)

    for group in groups:
        walk(group)

    if not subject_ids:
        return set()

    assignments = await er_client.get_source_assignments(subject_ids=sorted(subject_ids))
    return {str(a["source"]) for a in assignments if a.get("source")}
```

### 4. `app/actions/handlers.py` — `action_show_permissions`

Add raw copy-pasteable values alongside the existing human-readable lists.

```python
response["data"]["Event Type Slugs"] = sorted({
    event_type["value"]
    for event_type in event_types_response
    if event_type.get("value")
})
response["data"]["Event Category Slugs"] = sorted({
    event_type["category"]["value"]
    for event_type in event_types_response
    if event_type.get("category", {}).get("value")
})
# Two parallel views: name-keyed dicts for reading, raw list for copying.
response["data"]["Subject Group IDs"] = [
    {"name": g["name"], "id": g["id"]}
    for g in sorted(subject_groups_response, key=lambda g: g["name"])
]
response["data"]["Subject Group UUIDs"] = sorted([g["id"] for g in subject_groups_response])
```

The existing display-name lists stay readable for humans. The `DynamicJSONCard` widget already renders this shape — no portal change needed.

### 5. Tests — `app/actions/tests/test_actions.py`

- `test_pull_events_with_event_type_filter` — asserts `event_type` array is present in the JSON filter passed to `er_client.get_events`.
- `test_pull_events_with_event_category_filter`.
- `test_pull_observations_resolves_groups_to_sources_and_filters_inprocess` — feeds a mixed batch; asserts only obs whose `source` is in the resolved set are forwarded to Gundi.
- `test_resolve_source_ids_dedups_across_overlapping_groups` — same subject in two configured groups; subject_id added once; one `get_source_assignments` call.
- `test_resolve_source_ids_walks_subgroups` — parent group UUID + sub-group members → all subject_ids included before the source lookup.
- `test_pull_observations_emits_activity_log_when_groups_resolve_to_zero` — patches `log_activity`, asserts `LogLevel.ERROR`, `set_state` is not called, return dict has `filter_active=True, sources_resolved=0`.
- `test_pull_observations_no_filter_pulls_all` — empty `subject_group_ids` returns every observation; `filter_active=False`.

Mock surfaces: `AsyncERClient.get_events`, `.get_observations`, `.get_subjectgroups`, `.get_source_assignments`, plus `send_*_to_gundi` and `log_activity`.

## Rollout

No breaking change: empty filter = no constraint, same as the rest of the codebase. Existing prod integrations continue running as-is — they have empty lists for the new fields, which means "pull everything" exactly like today.

1. Deploy to dev; smoke against a known ER integration with explicit filters.
2. Deploy to stage; run pull_events + pull_observations against the CMORE-bound dev ER provider.
3. Deploy to prod.

## Out of scope (follow-up tickets)

- **Dynamic-choice form for event types and subject groups.** Long-term: portal calls a discovery endpoint and renders multi-select dropdowns populated from the user's ER account. Requires a portal-side change; file a separate Jira.
- **Server-side observation filtering.** If ER's observations endpoint grows multi-subject or multi-source filtering, push the filter there and remove the in-process pass.
- **Event attachments** flowing through `send_event_attachments_to_gundi`. File as `Pull ER event attachments`.
- **`gundi-client-v2` 3.x bump** for this repo. Same coordinated upgrade story as GUNDI-5372. Separate ticket.
- **Concurrent-run advisory lock.** The existing `last_execution` watermark race (a second scheduled run firing before the first finishes) is inherited, not introduced by this change. Worth a small Redis-SETNX guard, but separate ticket.
