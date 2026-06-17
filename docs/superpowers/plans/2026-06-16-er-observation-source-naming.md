# Time-Accurate Source Naming for ER Observations — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the meaningless `er-src-{uuid}` source identifier on pulled ER observations with the device's natural `manufacturer_id` (`external_source_id`), the time-correct `Subject.name` (`source_name`), and `subject_type`.

**Architecture:** A new per-run `SourceProfileResolver` (in `app/actions/source_profiles.py`) lazily fetches and caches, per ER source UUID, the device `manufacturer_id` and the full subject-assignment history (`assigned_range` + subject name/type). `transform_observations_to_gundi_schema` consults it to label each observation with the subject assigned at that observation's `recorded_at`. Pure range-matching logic is separated from I/O so it is unit-testable without mocks.

**Tech Stack:** Python 3.10, Pydantic v1 (matches `gundi_core`), `pytest` + `pytest-asyncio` + `pytest-mock`, the async `erclient`.

---

## Spec

Implements `docs/superpowers/specs/2026-06-16-er-observation-source-naming-design.md`. Read it first.

## Confirmed ER data shapes (from `das` serializers + `erclient`)

- `get_source_assignments(source_ids=[...])` → `GET /subjectsources?sources=uuid1,uuid2` → list of
  `{"id", "assigned_range": {"lower": iso, "upper": iso|null}, "source": "<uuid>", "subject": "<uuid>", ...}`.
- `get_source_subjects(source_id)` → `GET source/{id}/subjects` → list of subject dicts with `{"id", "name", "subject_type", "subject_subtype", ...}`.
- Source `manufacturer_id` lives on the source detail (`SourceSerializer`: `{"id", "manufacturer_id", ...}`), fetched via `GET source/{uuid}/` (the `erclient.get_source_by_manufacturer_id` method hits this path; ER resolves it by PK or manufacturer_id).

## File structure

- **Create** `app/actions/source_profiles.py` — `Assignment`, `SourceProfile`, `ResolvedSource` (Pydantic), the pure `resolve_source(...)` helper, and `SourceProfileResolver` (I/O). Self-contained; keeps the already-large `handlers.py` from growing.
- **Create** `app/actions/tests/test_source_profiles.py` — unit tests for the models, `resolve_source`, and the resolver (mocked `erclient`).
- **Modify** `app/actions/handlers.py` — `transform_observations_to_gundi_schema` signature/body; `_pull_source_window` to build+thread the resolver; `action_pull_observations` to construct one resolver per run.
- **Modify** `app/actions/tests/test_actions.py` — transform tests + a `pull_observations` wiring test.
- **Modify** `docs/actions/pull-observations.md` and `docs/data-flow.md` — document the new fields.

---

## Task 1: Spike — confirm fetch shapes against stage ER, capture fixtures

**Files:**
- Create: `app/actions/tests/fixtures/subjectsources_sample.json` (recorded real responses)

- [ ] **Step 1: Capture real responses from stage ER**

Using stage credentials (see `local/.env.local`), in a Python REPL or scratch script, call against a source UUID that has at least one subject assignment:

```python
import asyncio
from erclient import AsyncERClient
async def main():
    c = AsyncERClient(service_root=..., token=..., ...)
    async with c as er:
        print("ASSIGNMENTS:", await er.get_source_assignments(source_ids=["<src-uuid>"]))
        print("SUBJECTS:", await er.get_source_subjects("<src-uuid>"))
        print("SOURCE:", await er.get_source_by_manufacturer_id("<src-uuid>"))  # source/{uuid}/
asyncio.run(main())
```

- [ ] **Step 2: Confirm and record these facts** (write them into the spec's "implementation spike" note and save the JSON to the fixture file):
  - `assigned_range` exact keys and ISO format; whether `upper` is `null` for an open (current) assignment.
  - Whether `get_source_assignments` returns **historical** (past) assignments, not just current.
  - Whether `get_source_subjects` includes subjects from **past** assignments or only currently-linked ones (affects whether we can name a historically-assigned-but-now-unlinked subject).
  - The exact key for source `manufacturer_id` in the source-detail response.

- [ ] **Step 3: Commit the fixture**

```bash
git add app/actions/tests/fixtures/subjectsources_sample.json
git commit -m "test: capture ER subjectsources/source/subjects fixtures for source-naming"
```

> If Step 2 reveals `get_source_assignments` does NOT return historical ranges, stop and revisit the spec with the user — time-accuracy would need a different endpoint.

---

## Task 2: Data models

**Files:**
- Create: `app/actions/source_profiles.py`
- Test: `app/actions/tests/test_source_profiles.py`

- [ ] **Step 1: Write the failing test**

```python
# app/actions/tests/test_source_profiles.py
from datetime import datetime, timezone
from app.actions.source_profiles import Assignment, SourceProfile, ResolvedSource


def _dt(s):
    return datetime.fromisoformat(s).replace(tzinfo=timezone.utc)


def test_assignment_covers_open_ended():
    a = Assignment(lower=_dt("2026-01-01T00:00:00"), upper=None, subject_name="Tau", subject_type="elephant")
    assert a.covers(_dt("2026-06-01T00:00:00")) is True
    assert a.covers(_dt("2025-12-31T23:59:59")) is False


def test_assignment_covers_closed_range_half_open():
    a = Assignment(lower=_dt("2026-01-01T00:00:00"), upper=_dt("2026-02-01T00:00:00"))
    assert a.covers(_dt("2026-01-15T00:00:00")) is True
    assert a.covers(_dt("2026-02-01T00:00:00")) is False  # upper exclusive
    assert a.covers(_dt("2025-12-31T00:00:00")) is False
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest app/actions/tests/test_source_profiles.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'app.actions.source_profiles'`

- [ ] **Step 3: Write minimal implementation**

```python
# app/actions/source_profiles.py
import logging
from datetime import datetime
from typing import Iterable, List, Optional

from pydantic import BaseModel

logger = logging.getLogger(__name__)

# ER's /subjectsources accepts a comma-joined list of source UUIDs; keep chunks
# small so the query string can't 414 and the (single-page) response isn't truncated.
SOURCE_ID_CHUNK_SIZE = 25


class Assignment(BaseModel):
    lower: datetime
    upper: Optional[datetime] = None  # None = still assigned (open-ended)
    subject_name: Optional[str] = None
    subject_type: Optional[str] = None

    def covers(self, when: datetime) -> bool:
        if when < self.lower:
            return False
        if self.upper is not None and when >= self.upper:  # half-open [lower, upper)
            return False
        return True


class SourceProfile(BaseModel):
    manufacturer_id: Optional[str] = None
    assignments: List[Assignment] = []


class ResolvedSource(BaseModel):
    external_source_id: str
    source_name: Optional[str] = None
    subject_type: Optional[str] = None
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest app/actions/tests/test_source_profiles.py -v`
Expected: PASS (2 passed)

- [ ] **Step 5: Commit**

```bash
git add app/actions/source_profiles.py app/actions/tests/test_source_profiles.py
git commit -m "feat: add source-profile data models for ER observation naming"
```

---

## Task 3: Pure `resolve_source` range-matching

**Files:**
- Modify: `app/actions/source_profiles.py`
- Test: `app/actions/tests/test_source_profiles.py`

- [ ] **Step 1: Write the failing tests**

```python
# append to app/actions/tests/test_source_profiles.py
from app.actions.source_profiles import resolve_source


def test_resolve_no_profile_falls_back_to_uuid():
    r = resolve_source(None, "abc-123", _dt("2026-06-01T00:00:00"))
    assert r.external_source_id == "er-src-abc-123"
    assert r.source_name is None and r.subject_type is None


def test_resolve_missing_manufacturer_id_falls_back_to_uuid():
    p = SourceProfile(manufacturer_id=None, assignments=[
        Assignment(lower=_dt("2026-01-01T00:00:00"), upper=None, subject_name="Tau", subject_type="elephant"),
    ])
    r = resolve_source(p, "abc-123", _dt("2026-06-01T00:00:00"))
    assert r.external_source_id == "er-src-abc-123"
    assert r.source_name == "Tau" and r.subject_type == "elephant"


def test_resolve_uses_manufacturer_id_and_time_correct_subject():
    p = SourceProfile(manufacturer_id="SERIAL-9", assignments=[
        Assignment(lower=_dt("2026-01-01T00:00:00"), upper=_dt("2026-03-01T00:00:00"), subject_name="Tau", subject_type="elephant"),
        Assignment(lower=_dt("2026-03-01T00:00:00"), upper=None, subject_name="Habi", subject_type="elephant"),
    ])
    early = resolve_source(p, "abc-123", _dt("2026-02-01T00:00:00"))
    late = resolve_source(p, "abc-123", _dt("2026-06-01T00:00:00"))
    assert early.external_source_id == "SERIAL-9" and early.source_name == "Tau"
    assert late.source_name == "Habi"


def test_resolve_no_covering_assignment_omits_name():
    p = SourceProfile(manufacturer_id="SERIAL-9", assignments=[
        Assignment(lower=_dt("2026-03-01T00:00:00"), upper=None, subject_name="Habi"),
    ])
    r = resolve_source(p, "abc-123", _dt("2026-01-01T00:00:00"))
    assert r.external_source_id == "SERIAL-9"
    assert r.source_name is None and r.subject_type is None


def test_resolve_overlapping_picks_latest_starting():
    p = SourceProfile(manufacturer_id="S", assignments=[
        Assignment(lower=_dt("2026-01-01T00:00:00"), upper=None, subject_name="Old"),
        Assignment(lower=_dt("2026-02-01T00:00:00"), upper=None, subject_name="New"),
    ])
    r = resolve_source(p, "abc-123", _dt("2026-06-01T00:00:00"))
    assert r.source_name == "New"
```

- [ ] **Step 2: Run to verify it fails**

Run: `pytest app/actions/tests/test_source_profiles.py -k resolve -v`
Expected: FAIL — `ImportError: cannot import name 'resolve_source'`

- [ ] **Step 3: Implement**

```python
# append to app/actions/source_profiles.py
def resolve_source(profile: Optional[SourceProfile], source_uuid: str, recorded_at: datetime) -> ResolvedSource:
    """Pure: map a source UUID + observation time to its Gundi identifiers.

    external_source_id = manufacturer_id, falling back to er-src-{uuid}. The
    subject name/type come from the assignment whose half-open [lower, upper)
    range contains recorded_at; on overlap the latest-starting assignment wins.
    """
    fallback = f"er-src-{source_uuid}"
    if profile is None:
        return ResolvedSource(external_source_id=fallback)
    external = profile.manufacturer_id or fallback
    covering = [a for a in profile.assignments if a.covers(recorded_at)]
    chosen = max(covering, key=lambda a: a.lower) if covering else None
    return ResolvedSource(
        external_source_id=external,
        source_name=chosen.subject_name if chosen else None,
        subject_type=chosen.subject_type if chosen else None,
    )
```

- [ ] **Step 4: Run to verify it passes**

Run: `pytest app/actions/tests/test_source_profiles.py -v`
Expected: PASS (all)

- [ ] **Step 5: Commit**

```bash
git add app/actions/source_profiles.py app/actions/tests/test_source_profiles.py
git commit -m "feat: add time-accurate resolve_source range matching"
```

---

## Task 4: `SourceProfileResolver` — fetch + cache

**Files:**
- Modify: `app/actions/source_profiles.py`
- Test: `app/actions/tests/test_source_profiles.py`

- [ ] **Step 1: Write the failing test** (mock the erclient; assignment ranges joined to subject detail + manufacturer_id)

```python
# append to app/actions/tests/test_source_profiles.py
import pytest
from app.actions.source_profiles import SourceProfileResolver


class _FakeER:
    def __init__(self):
        self.assignment_calls = []
        self.source_detail_calls = []
    async def get_source_assignments(self, subject_ids=None, source_ids=None):
        self.assignment_calls.append(tuple(source_ids or []))
        return [{
            "assigned_range": {"lower": "2026-01-01T00:00:00+00:00", "upper": None},
            "source": "src-1", "subject": "subj-1",
        }]
    async def get_source_subjects(self, source_id):
        return [{"id": "subj-1", "name": "Tau", "subject_type": "elephant"}]
    async def get_source_by_manufacturer_id(self, source_id):  # source/{uuid}/
        self.source_detail_calls.append(source_id)
        return {"id": "src-1", "manufacturer_id": "SERIAL-9"}


@pytest.mark.asyncio
async def test_resolver_builds_profile_and_caches():
    er = _FakeER()
    r = SourceProfileResolver(er)
    await r.ensure(["src-1"])
    await r.ensure(["src-1"])  # second call must hit cache, not refetch
    assert er.source_detail_calls == ["src-1"]            # fetched once
    assert er.assignment_calls == [("src-1",)]            # fetched once
    res = r.resolve("src-1", _dt("2026-06-01T00:00:00"))
    assert res.external_source_id == "SERIAL-9"
    assert res.source_name == "Tau" and res.subject_type == "elephant"


@pytest.mark.asyncio
async def test_resolver_fetch_error_falls_back(mocker):
    er = _FakeER()
    mocker.patch.object(er, "get_source_assignments", side_effect=RuntimeError("boom"))
    r = SourceProfileResolver(er)
    await r.ensure(["src-1"])
    res = r.resolve("src-1", _dt("2026-06-01T00:00:00"))
    assert res.external_source_id == "er-src-src-1"       # fallback, never raises
    assert res.source_name is None
```

- [ ] **Step 2: Run to verify it fails**

Run: `pytest app/actions/tests/test_source_profiles.py -k resolver -v`
Expected: FAIL — `ImportError: cannot import name 'SourceProfileResolver'`

- [ ] **Step 3: Implement**

```python
# append to app/actions/source_profiles.py
def _chunked(seq, size):
    seq = list(seq)
    for i in range(0, len(seq), size):
        yield seq[i:i + size]


def _parse_dt(value):
    from datetime import datetime
    return datetime.fromisoformat(value) if value else None


class SourceProfileResolver:
    """Per-run resolver: source UUID -> SourceProfile, fetched lazily and cached.

    Covers both pull paths because it keys off the source UUIDs that actually
    appear in observations. Enrichment failures degrade to a UUID fallback and
    are never allowed to fail the pull.
    """

    def __init__(self, er_client, *, integration_id=None):
        self._er = er_client
        self._integration_id = integration_id
        self._cache = {}

    async def ensure(self, source_uuids: Iterable[str]) -> None:
        missing = sorted({u for u in source_uuids if u and u not in self._cache})
        if not missing:
            return
        ranges_by_source = await self._fetch_ranges(missing)
        for uuid in missing:
            try:
                self._cache[uuid] = await self._build_profile(uuid, ranges_by_source.get(uuid, []))
            except Exception as e:
                logger.warning(
                    "Source profile fetch failed for %s (%s); using UUID fallback.",
                    uuid, e, extra={"attention_needed": True},
                )
                self._cache[uuid] = SourceProfile()

    def resolve(self, source_uuid, recorded_at) -> ResolvedSource:
        return resolve_source(self._cache.get(source_uuid), source_uuid, recorded_at)

    async def _fetch_ranges(self, source_uuids):
        """source UUID -> list of (lower, upper, subject_uuid) from /subjectsources."""
        out = {}
        for chunk in _chunked(source_uuids, SOURCE_ID_CHUNK_SIZE):
            try:
                raw = await self._er.get_source_assignments(source_ids=list(chunk))
            except Exception:
                raise  # handled per-source by ensure() fallback below
            records = raw.get("results", []) if isinstance(raw, dict) else raw
            for rec in records or []:
                src = rec.get("source")
                rng = rec.get("assigned_range") or {}
                if not src:
                    continue
                out.setdefault(src, []).append(
                    (_parse_dt(rng.get("lower")), _parse_dt(rng.get("upper")), rec.get("subject"))
                )
        return out

    async def _build_profile(self, source_uuid, ranges):
        detail = await self._er.get_source_by_manufacturer_id(source_uuid)  # source/{uuid}/
        manufacturer_id = (detail or {}).get("manufacturer_id")
        subjects = await self._er.get_source_subjects(source_uuid)
        subj_by_id = {s.get("id"): s for s in (subjects or [])}
        assignments = []
        for lower, upper, subject_uuid in ranges:
            if lower is None:
                continue
            s = subj_by_id.get(subject_uuid, {})
            assignments.append(Assignment(
                lower=lower, upper=upper,
                subject_name=s.get("name"),
                subject_type=s.get("subject_type"),
            ))
        return SourceProfile(manufacturer_id=manufacturer_id, assignments=assignments)
```

> The exact join (whether `get_source_subjects` covers historically-assigned subjects) is what Task 1 confirms. If past subjects are missing from it, replace the `get_source_subjects` lookup with per-subject detail fetches keyed by the assignment's `subject` UUID.

- [ ] **Step 4: Run to verify it passes**

Run: `pytest app/actions/tests/test_source_profiles.py -v`
Expected: PASS (all)

- [ ] **Step 5: Commit**

```bash
git add app/actions/source_profiles.py app/actions/tests/test_source_profiles.py
git commit -m "feat: add SourceProfileResolver with lazy fetch, cache, and fallback"
```

---

## Task 5: Use the resolver in the observation transform

**Files:**
- Modify: `app/actions/handlers.py` (`transform_observations_to_gundi_schema`, ~line 1303)
- Test: `app/actions/tests/test_actions.py`

- [ ] **Step 1: Write the failing test**

```python
# append to app/actions/tests/test_actions.py
from datetime import datetime, timezone
from app.actions.handlers import transform_observations_to_gundi_schema
from app.actions.source_profiles import SourceProfile, Assignment, SourceProfileResolver


class _StaticResolver:
    def __init__(self, profile):
        self._p = profile
    def resolve(self, source_uuid, recorded_at):
        from app.actions.source_profiles import resolve_source
        return resolve_source(self._p, source_uuid, recorded_at)


def test_transform_observation_enriches_with_profile():
    prof = SourceProfile(manufacturer_id="SERIAL-9", assignments=[
        Assignment(lower=datetime(2026,1,1,tzinfo=timezone.utc), upper=None, subject_name="Tau", subject_type="elephant"),
    ])
    obs = [{
        "source": "src-1",
        "recorded_at": "2026-06-01T00:00:00+00:00",
        "location": {"longitude": -72.7, "latitude": -51.7},
        "speed_kmph": 4,
    }]
    out = transform_observations_to_gundi_schema(obs, resolver=_StaticResolver(prof))
    assert out[0]["source"] == "SERIAL-9"
    assert out[0]["source_name"] == "Tau"
    assert out[0]["subject_type"] == "elephant"
    assert out[0]["location"] == {"lon": -72.7, "lat": -51.7}
    assert out[0]["additional"]["er_source_id"] == "src-1"
    assert out[0]["additional"]["speed_kmph"] == 4


def test_transform_observation_without_resolver_keeps_legacy_fallback():
    obs = [{"source": "src-1", "recorded_at": "2026-06-01T00:00:00+00:00"}]
    out = transform_observations_to_gundi_schema(obs)
    assert out[0]["source"] == "er-src-src-1"
    assert "source_name" not in out[0]
```

- [ ] **Step 2: Run to verify it fails**

Run: `pytest app/actions/tests/test_actions.py -k transform_observation -v`
Expected: FAIL — `transform_observations_to_gundi_schema() got an unexpected keyword argument 'resolver'`

- [ ] **Step 3: Implement** — replace the body of `transform_observations_to_gundi_schema` (handlers.py:1303-1331)

```python
def transform_observations_to_gundi_schema(observations, resolver=None):
    transformed_data = []
    for observation in observations:
        try:
            transformed_observation = {}
            recorded_at = observation.get("recorded_at")
            if recorded_at:
                transformed_observation["recorded_at"] = recorded_at
            source_uuid = observation.get("source")
            if source_uuid:
                if resolver is not None:
                    from datetime import datetime
                    when = datetime.fromisoformat(recorded_at) if recorded_at else None
                    resolved = resolver.resolve(source_uuid, when)
                    transformed_observation["source"] = resolved.external_source_id
                    if resolved.source_name:
                        transformed_observation["source_name"] = resolved.source_name
                    if resolved.subject_type:
                        transformed_observation["subject_type"] = resolved.subject_type
                else:
                    transformed_observation["source"] = f"er-src-{source_uuid}"
            if location := observation.get("location"):
                transformed_observation["location"] = {
                    "lon": location.get("longitude"),
                    "lat": location.get("latitude"),
                }
            # Everything not already mapped goes to additional; preserve the raw
            # ER source UUID for traceability/reconciliation after the identity change.
            additional = {
                key: value for key, value in observation.items()
                if key not in transformed_observation.keys() and key != "source"
            }
            if source_uuid:
                additional["er_source_id"] = source_uuid
            transformed_observation["additional"] = additional
        except Exception as e:
            logger.error(
                f"Error transforming observation {observation}: {e}",
                extra={"attention_needed": True},
            )
            continue
        else:
            transformed_data.append(transformed_observation)
    return transformed_data
```

- [ ] **Step 4: Run to verify it passes**

Run: `pytest app/actions/tests/test_actions.py -k transform_observation -v`
Expected: PASS (2 passed)

- [ ] **Step 5: Commit**

```bash
git add app/actions/handlers.py app/actions/tests/test_actions.py
git commit -m "feat: enrich observation transform with source profile (manufacturer_id + subject)"
```

---

## Task 6: Construct + thread the resolver through `pull_observations`

**Files:**
- Modify: `app/actions/handlers.py` (`_pull_source_window` ~line 989; `action_pull_observations` ~line 610)
- Test: `app/actions/tests/test_actions.py`

- [ ] **Step 1: Write the failing test** (resolver is created and `_pull_source_window` enriches)

```python
# append to app/actions/tests/test_actions.py
import pytest
from app.actions.tests.conftest import AsyncIterator  # existing helper
from app.actions import handlers


@pytest.mark.asyncio
async def test_pull_source_window_enriches_via_resolver(mocker):
    er = mocker.MagicMock()
    er.get_observations.return_value = AsyncIterator([[
        {"source": "src-1", "recorded_at": "2026-06-01T00:00:00+00:00",
         "location": {"longitude": 1.0, "latitude": 2.0}},
    ]])
    sent = {}
    async def fake_send(observations, integration_id):
        sent["observations"] = observations
        return {}
    mocker.patch.object(handlers, "send_observations_to_gundi", side_effect=fake_send)

    resolver = mocker.MagicMock()
    from app.actions.source_profiles import ResolvedSource
    resolver.ensure = mocker.AsyncMock()
    resolver.resolve.return_value = ResolvedSource(
        external_source_id="SERIAL-9", source_name="Tau", subject_type="elephant")

    count = await handlers._pull_source_window(
        er, "src-1", "2026-06-01T00:00:00+00:00", "2026-06-02T00:00:00+00:00",
        integration_id="int-1", resolver=resolver,
    )
    assert count == 1
    resolver.ensure.assert_awaited()                       # sources prefetched
    assert sent["observations"][0]["source"] == "SERIAL-9"
    assert sent["observations"][0]["source_name"] == "Tau"
```

- [ ] **Step 2: Run to verify it fails**

Run: `pytest app/actions/tests/test_actions.py -k pull_source_window_enriches -v`
Expected: FAIL — `_pull_source_window() got an unexpected keyword argument 'resolver'`

- [ ] **Step 3: Implement**

In `_pull_source_window` (handlers.py:989), add a `resolver=None` keyword param. After fetching each `observations` batch and before transforming, prefetch profiles and pass the resolver through:

```python
async def _pull_source_window(er_client, source, start, end, *, integration_id, resolver=None):
    params = {"start": start, "end": end, "batch_size": BATCH_SIZE}
    if source is not None:
        params["source_id"] = source
    total = 0
    async for observations in er_client.get_observations(**params):
        if resolver is not None:
            await resolver.ensure({o.get("source") for o in observations if o.get("source")})
        transformed = transform_observations_to_gundi_schema(observations, resolver=resolver)
        if transformed:
            await send_observations_to_gundi(observations=transformed, integration_id=integration_id)
        total += len(transformed)
    return total
```

(Preserve the existing batching/counting logic; only the `resolver` plumbing is new. Match the real function's current structure when editing.)

In `action_pull_observations` (handlers.py:610), construct one resolver per run and pass it into every `_pull_source_window` call:

```python
from app.actions.source_profiles import SourceProfileResolver
# ... inside the `async with er_client as earth_ranger:` block, before the unit loop:
resolver = SourceProfileResolver(earth_ranger, integration_id=integration_id)
# ... at the call site (the existing _pull_source_window invocation):
sent = await _pull_source_window(
    earth_ranger, source, w_start, w_end, integration_id=integration_id, resolver=resolver
)
```

- [ ] **Step 4: Run to verify it passes**

Run: `pytest app/actions/tests/test_actions.py -k "pull_source_window_enriches or pull_observations" -v`
Expected: PASS

- [ ] **Step 5: Run the full action suite for regressions**

Run: `pytest app/actions/tests/ -q`
Expected: PASS (all existing + new)

- [ ] **Step 6: Commit**

```bash
git add app/actions/handlers.py app/actions/tests/test_actions.py
git commit -m "feat: thread SourceProfileResolver through pull_observations"
```

---

## Task 7: Documentation

**Files:**
- Modify: `docs/data-flow.md` (Observations section)
- Modify: `docs/actions/pull-observations.md`

- [ ] **Step 1: Update `docs/data-flow.md`** — replace the Observations mapping table so it reads:

```markdown
| Gundi field | Source |
|-------------|--------|
| `source` (external_source_id) | `Source.manufacturer_id`, falling back to `er-src-<ER source UUID>` when absent. |
| `source_name` | `Subject.name` for the subject assigned at the observation's `recorded_at` (time-accurate via `assigned_range`). |
| `subject_type` | the assigned subject's type. |
| `recorded_at` | ER `recorded_at`. |
| `location` | ER `{longitude, latitude}` → `{lon, lat}`. |
| `additional.er_source_id` | the raw ER source UUID, preserved for traceability. |
| `additional` | remaining ER fields. |
```

Add a sentence: "Enrichment is best-effort — if the device or subject can't be resolved, the observation still sends under `er-src-<uuid>` with no name."

- [ ] **Step 2: Update `docs/actions/pull-observations.md`** — in the "What it does" section, change the line describing the transform to note `external_source_id = manufacturer_id` (fallback `er-src-{uuid}`), `source_name = time-accurate Subject.name`, and `subject_type`.

- [ ] **Step 3: Verify docs build**

Run: `python3 -m venv /tmp/erdocs && /tmp/erdocs/bin/pip install -q -r requirements-docs.txt && /tmp/erdocs/bin/mkdocs build --strict --site-dir /tmp/erdocs_site`
Expected: "Documentation built" with no warnings/errors.

- [ ] **Step 4: Commit**

```bash
git add docs/data-flow.md docs/actions/pull-observations.md
git commit -m "docs: document source-profile enrichment of ER observations"
```

---

## Final verification

- [ ] `pytest app/actions/tests/ -q` — all pass.
- [ ] PR description calls out the **identity change** (observations now keyed by `manufacturer_id`, not `er-src-{uuid}`; `additional.er_source_id` preserved) and links the spec.

## Notes for the implementer

- **Pydantic v1** (no `model_config`; use `class Config` if needed). Match the repo's existing models.
- **`recorded_at` parsing:** observations carry ISO-8601 strings. `datetime.fromisoformat` handles the `+00:00` offset. If any ER timestamps use `Z`, normalize before parsing (the spike fixture will reveal the format).
- **Never let enrichment fail the pull** — the resolver swallows per-source fetch errors into a UUID fallback. Keep that invariant.
- **Don't refactor** the surrounding backfill/cursor/lease logic in `_pull_source_window` / `action_pull_observations`; only add the `resolver` plumbing.
