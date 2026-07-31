# Design: time-accurate source naming for ER observations

**Date:** 2026-06-16
**Status:** Approved (pending spec review)
**Component:** `app/actions/handlers.py` — `pull_observations` observation transform

## Problem

Observations pulled from EarthRanger are forwarded to Gundi with only:

```python
transformed_observation["source"] = f"er-src-{source}"   # source = ER source PK (UUID)
```

`external_source_id` therefore carries the ER source **primary key**, which is meaningless to a
user reading the Gundi activity log, an ER map, or a downstream destination. No friendly name and no
natural device identifier are sent at all.

EarthRanger models three relevant identifiers:

| ER concept | Meaning |
|------------|---------|
| `Source.id` (UUID) | Primary key within ER — what we send today as `er-src-{uuid}`. |
| `Source.manufacturer_id` | Natural ID of the physical device (serial number, etc.), stable upstream of ER. |
| `Subject.name` | Friendly name shown on ER maps/reports. |

The Gundi v2 `Observation` schema already has slots for all of this (verified in `gundi_core.schemas.v2`):

| Input field | Schema field | Intended use |
|-------------|--------------|--------------|
| `source` | `external_source_id` | stable identity / dedup key |
| `source_name` | `source_name` | friendly display label |
| `subject_type` | `subject_type` | subject type |

We are simply not populating them.

## Goal

Populate the observation with meaningful identifiers (decision: **both** display and natural identity):

- `external_source_id` = `Source.manufacturer_id` (fall back to `er-src-{uuid}` when absent).
- `source_name` = `Subject.name`.
- `subject_type` = `Subject.subject_type`.

The subject must be **time-accurate**: a source/collar can be reassigned to different subjects over
time, and `pull_observations` backfills historical windows. Each observation is labelled with the
subject whose assignment `assigned_range` contains the observation's `recorded_at`.

## Decisions (locked during brainstorming)

1. **Both** friendly display *and* natural identity (not display-only).
2. **Time-accurate** subject mapping via `assigned_range`, not current-assignment.
3. **Approach A** — a per-run, lazily-populated, cached source-profile resolver threaded into the
   transform. Works for both the group-filtered and whole-instance pull paths because it keys off the
   source UUIDs that actually appear in observations.
4. **`external_source_id` fallback** — `manufacturer_id` when present, else `er-src-{uuid}` (so every
   source keeps a stable identity; we never emit an empty/duplicated identity).

### Approaches considered

- **A (chosen)** — lazy cached `SourceProfileResolver` threaded into the transform. Only approach that
  satisfies time-accurate labelling *and* both pull paths, and isolates enrichment as a testable unit.
- **B (rejected)** — extend `_resolve_source_ids` to emit profiles up front. Only works when
  `subject_group_ids` is configured (no whole-instance support), and the group walk yields *current*
  subjects, not the ranged assignment history time-accuracy needs.
- **C (rejected)** — have ER include subject/source inline on observations. ER's observation endpoint
  is source-scoped time-series; it returns neither `manufacturer_id` nor a *historical* subject.

## Design

### Component: `SourceProfileResolver`

Created once per `action_pull_observations` run; holds the ER client and an in-memory cache keyed by
source UUID. Lives in a new module `app/actions/source_profiles.py` — `handlers.py` is already ~1400
lines, and the resolver (plus its Pydantic models) is a self-contained, independently testable unit, so
it does not belong in the handler file.

- `async ensure(source_uuids: Iterable[str]) -> None` — for any UUID not already cached, batch-fetch its
  assignment history and `manufacturer_id`, build and store a `SourceProfile`. Idempotent; only fetches
  what's missing.
- `resolve(source_uuid: str, recorded_at: datetime) -> ResolvedSource` — pure (no I/O). Selects the
  assignment whose `assigned_range` contains `recorded_at` and returns:
  - `external_source_id` = `manufacturer_id` or `er-src-{uuid}`
  - `source_name` = matched `subject_name` or `None`
  - `subject_type` = matched `subject_type` or `None`

### Data model (Pydantic, per repo conventions — no dataclasses)

```text
SourceProfile {
    manufacturer_id: str | None
    assignments: list[Assignment]
}
Assignment {
    lower: datetime
    upper: datetime | None        # None = still assigned (open-ended)
    subject_name: str | None
    subject_type: str | None
}
ResolvedSource {
    external_source_id: str
    source_name: str | None
    subject_type: str | None
}
```

### Data flow / integration points

1. `action_pull_observations` constructs one `SourceProfileResolver(er_client)` and threads it into
   `_pull_source_window`.
2. `_pull_source_window` collects the batch's source UUIDs, calls `await resolver.ensure(uuids)`, then
   `transform_observations_to_gundi_schema(observations, resolver)`.
3. The transform, per observation:
   - `resolved = resolver.resolve(obs["source"], obs["recorded_at"])`
   - `transformed["source"] = resolved.external_source_id`
   - `transformed["source_name"] = resolved.source_name` (when present)
   - `transformed["subject_type"] = resolved.subject_type` (when present)
   - keeps `recorded_at` and `location` as today
   - `additional["er_source_id"] = obs["source"]` (preserve the raw ER UUID for traceability/reconciliation)
   - remaining ER fields continue into `additional` as today

This keys off source UUIDs seen in observations, so it covers both the group-filtered path and the
whole-instance (`sources = [None]`) path without special-casing.

### Fetching strategy & performance

- Batch `get_source_assignments(source_ids=chunk)` using the existing chunked pattern (add a
  `SOURCE_ID_CHUNK_SIZE` mirroring `SUBJECT_ID_CHUNK_SIZE`).
- **Implementation spike (must verify before coding the fetch):** confirm whether a `subjectsources`
  record embeds `subject.name` / `subject_type` and `source.manufacturer_id`. If yes, one batched call
  per chunk yields everything. If not, add a batched source-detail and/or subject-detail lookup. Record
  the finding in the implementation plan.
- Per-run cache keyed by source UUID prevents refetching within a run. On a resumed backfill chunk the
  cache is rebuilt from scratch (acceptable — read-only lookups, bounded by the sources in that chunk).

### Edge & error handling

| Case | Behavior |
|------|----------|
| No assignment range contains `recorded_at` | Omit `source_name` / `subject_type`; debug-log. Still emit the observation. |
| `manufacturer_id` missing | `external_source_id = er-src-{uuid}`. |
| Resolver fetch fails for a source | Fall back to `er-src-{uuid}`, no name; warn; **continue** (enrichment must never fail the pull). |
| Overlapping assignment ranges (bad ER data) | Pick the latest-starting assignment; warn. |
| Open-ended `assigned_range.upper` (null) | Treat as ongoing (matches any `recorded_at >= lower`). |

### Testing

- **Resolver unit tests:** range matching — observation before / within / after a range, open `upper`,
  gaps between ranges, overlapping ranges; `manufacturer_id` present vs missing → fallback.
- **Transform unit tests:** with a profile → `source`/`source_name`/`subject_type` set and
  `additional.er_source_id` preserved; without a profile / on resolver miss → `er-src-{uuid}` fallback.
- **`pull_observations` test:** resolver is constructed and threaded through; `get_source_assignments`
  is queried for the observed sources (mocked ER client).

## Backward compatibility / migration

Changing `external_source_id` from `er-src-{uuid}` to `manufacturer_id` **changes source identity**
downstream. Observations already ingested under `er-src-{uuid}` will not stitch together with new ones
keyed by `manufacturer_id`; destinations (e.g. C-more GNodes) may register *new* sources. This is the
accepted cost of the more-correct identity. The raw ER UUID is preserved in `additional.er_source_id`
to support any downstream reconciliation. Call this out in the PR description.

## Out of scope

- Backfilling/relabelling observations already sent under the old `er-src-{uuid}` identity.
- Persisting the source-profile cache across runs (per-run rebuild is sufficient).
- Any change to event (non-observation) transforms.
