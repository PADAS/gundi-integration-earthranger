# Resumable ER Observation Backfill — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `action_pull_observations` fetch only the configured sources' data server-side, process an arbitrarily large historical window in bounded, resumable chunks, fix source-assignment pagination, and prevent overlapping scheduled runs from double-processing.

**Architecture:** Observations are pulled as discrete `(time sub-window × source)` units. ER filters by a single `source_id` per request, so we loop per-source instead of pulling the whole instance and filtering in Python. A Redis-backed cursor records progress `(window_index, source_index)` and is committed after each unit; a soft time budget yields control before the hard `asyncio.wait_for` timeout, returning `in_progress` so the next invocation resumes. A per-`(integration, action)` Redis lease (`SET NX EX`) makes overlapping invocations harmless no-ops. Continuation is scheduler-driven by default, with an opt-in PubSub self-re-trigger.

**Tech Stack:** Python 3.10, FastAPI, `erclient.AsyncERClient`, Redis (`redis.asyncio`), `python-dateutil`, pytest + pytest-asyncio + pytest-mock.

**Spec:** `docs/superpowers/specs/2026-06-13-er-observations-resumable-backfill-design.md`

---

## File Structure

| File | Responsibility | Change |
|------|----------------|--------|
| `app/actions/handlers.py` | Pull-observation orchestration + helpers | Modify (bulk of work) |
| `app/actions/configurations.py` | `PullObservationsConfig` fields | Modify (2 new fields) |
| `app/actions/tests/conftest.py` | Shared fixtures | Modify (`mock_state_manager` gains `set_if_absent`/`delete_state`) |
| `app/actions/tests/test_actions.py` | Action tests | Modify (update 3 existing e2e tests, add unit + e2e tests) |

### New module-level constants (in `app/actions/handlers.py`)

```python
BUDGET_FRACTION = 0.8            # fraction of MAX_ACTION_EXECUTION_TIME before yielding
SUBJECT_ID_CHUNK_SIZE = 25       # subjects per subjectsources request
MAX_NO_PROGRESS_RETRIES = 3      # self-re-trigger runaway guard
LOCK_MARGIN_SECONDS = 30         # lease TTL margin above the hard timeout
BACKFILL_LOCK_SOURCE_ID = "backfill-lock"
```

### New helpers (in `app/actions/handlers.py`)

- `_chunked(seq, size)` — yield fixed-size chunks.
- `_parse_iso(s)` / `_to_iso(dt)` — tolerant ISO-8601 round-tripping (`dateutil.parser.isoparse`).
- `_iter_subwindows(start_iso, end_iso, subwindow_days)` — ascending half-open windows.
- `_fetch_source_assignments(er_client, subject_ids, *, integration_id=None)` — chunked subjectsources fetch with pagination-`next` warning and malformed-record diagnostics.
- `_acquire_backfill_lease(integration_id)` / `_release_backfill_lease(integration_id)`.
- `_build_backfill_cursor(...)` / `_save_backfill_cursor(...)`.
- `_pull_source_window(er_client, source, start, end, integration_id)` — drain one unit.

---

## Task 1: Add lease/cursor primitives to the state-manager test fixture

The new handler calls `state_manager.set_if_absent(...)` (lease) and `state_manager.delete_state(...)` (lease release + cursor clear). The shared `mock_state_manager` fixture only stubs `get_state`/`set_state`, so awaiting the others would fail. Add them so every existing and new pull-observations test keeps working.

**Files:**
- Modify: `app/actions/tests/conftest.py:269-276`

- [ ] **Step 1: Update the fixture**

Replace the existing `mock_state_manager` fixture (lines 269-276) with:

```python
@pytest.fixture
def mock_state_manager(mocker):
    mock_state_manager = mocker.MagicMock()
    mock_state_manager.get_state.return_value = async_return(
        {'last_execution': '2023-11-17T11:20:00+0200'}
    )
    mock_state_manager.set_state.return_value = async_return(None)
    # Backfill lease + cursor-clear primitives. Default: lease acquired.
    mock_state_manager.set_if_absent.return_value = async_return(True)
    mock_state_manager.delete_state.return_value = async_return(None)
    return mock_state_manager
```

- [ ] **Step 2: Run the existing observation test to confirm the fixture still wires up**

Run: `pytest app/actions/tests/test_actions.py::test_execute_pull_observations_action -v`
Expected: PASS (behavior unchanged at this point; we only added unused stubs).

- [ ] **Step 3: Commit**

```bash
git add app/actions/tests/conftest.py
git commit -m "test: add set_if_absent/delete_state to mock_state_manager fixture"
```

---

## Task 2: `_chunked` utility

**Files:**
- Modify: `app/actions/handlers.py` (add helper near the other auxiliary functions, after `_as_list`)
- Test: `app/actions/tests/test_actions.py`

- [ ] **Step 1: Write the failing test**

Add to `app/actions/tests/test_actions.py`:

```python
def test_chunked_splits_into_fixed_size_groups():
    from app.actions.handlers import _chunked
    assert list(_chunked([1, 2, 3, 4, 5], 2)) == [[1, 2], [3, 4], [5]]


def test_chunked_empty_yields_nothing():
    from app.actions.handlers import _chunked
    assert list(_chunked([], 3)) == []
```

- [ ] **Step 2: Run to verify it fails**

Run: `pytest app/actions/tests/test_actions.py::test_chunked_splits_into_fixed_size_groups -v`
Expected: FAIL with `ImportError: cannot import name '_chunked'`.

- [ ] **Step 3: Implement**

Add to `app/actions/handlers.py` (in the auxiliary-functions section, right after `_as_list`):

```python
def _chunked(seq, size):
    """Yield successive ``size``-length chunks (lists) from ``seq``."""
    seq = list(seq)
    for i in range(0, len(seq), size):
        yield seq[i:i + size]
```

- [ ] **Step 4: Run to verify it passes**

Run: `pytest app/actions/tests/test_actions.py -k chunked -v`
Expected: PASS (2 tests).

- [ ] **Step 5: Commit**

```bash
git add app/actions/handlers.py app/actions/tests/test_actions.py
git commit -m "feat: add _chunked helper"
```

---

## Task 3: Time sub-window iteration

`datetime.fromisoformat` in Python 3.10 cannot parse ER timestamps like `2023-11-17T11:20:00+0200`, so we use `dateutil.parser.isoparse`.

**Files:**
- Modify: `app/actions/handlers.py` (add `import datetime` already present; add `from dateutil import parser as dateutil_parser` to imports; add helpers)
- Test: `app/actions/tests/test_actions.py`

- [ ] **Step 1: Write the failing tests**

Add to `app/actions/tests/test_actions.py`:

```python
def test_iter_subwindows_splits_window_by_days():
    from app.actions.handlers import _iter_subwindows
    windows = _iter_subwindows("2025-01-01T00:00:00+00:00", "2025-01-03T00:00:00+00:00", 1)
    assert windows == [
        ("2025-01-01T00:00:00+00:00", "2025-01-02T00:00:00+00:00"),
        ("2025-01-02T00:00:00+00:00", "2025-01-03T00:00:00+00:00"),
    ]


def test_iter_subwindows_last_window_clipped_to_end():
    from app.actions.handlers import _iter_subwindows
    windows = _iter_subwindows("2025-01-01T00:00:00+00:00", "2025-01-02T12:00:00+00:00", 1)
    assert windows[-1] == ("2025-01-02T00:00:00+00:00", "2025-01-02T12:00:00+00:00")


def test_iter_subwindows_handles_er_offset_without_colon():
    from app.actions.handlers import _iter_subwindows
    # ER emits offsets like +0200 (no colon); must not raise.
    windows = _iter_subwindows("2023-11-17T11:20:00+0200", "2023-11-18T11:20:00+0200", 1)
    assert len(windows) == 1


def test_iter_subwindows_empty_when_start_not_before_end():
    from app.actions.handlers import _iter_subwindows
    assert _iter_subwindows("2025-01-02T00:00:00+00:00", "2025-01-01T00:00:00+00:00", 1) == []


def test_iter_subwindows_floors_subwindow_days_to_one():
    from app.actions.handlers import _iter_subwindows
    windows = _iter_subwindows("2025-01-01T00:00:00+00:00", "2025-01-02T00:00:00+00:00", 0)
    assert len(windows) == 1
```

- [ ] **Step 2: Run to verify they fail**

Run: `pytest app/actions/tests/test_actions.py -k iter_subwindows -v`
Expected: FAIL with `ImportError: cannot import name '_iter_subwindows'`.

- [ ] **Step 3: Implement**

Add `from dateutil import parser as dateutil_parser` to the import block at the top of `app/actions/handlers.py` (alongside the other third-party imports). Then add these helpers in the auxiliary-functions section:

```python
def _parse_iso(value):
    """Parse an ISO-8601 string (tolerant of ER's no-colon offsets and 'Z')."""
    return dateutil_parser.isoparse(value)


def _to_iso(dt):
    """Render a datetime back to ISO-8601."""
    return dt.isoformat()


def _iter_subwindows(start_iso, end_iso, subwindow_days):
    """Return ascending half-open ``[start, end)`` sub-windows as ISO pairs.

    The window list is deterministic given (start, end, subwindow_days), so a
    resumed run regenerates the exact same units and continues by index.
    Returns an empty list when ``start`` is not before ``end``.
    """
    days = max(1, int(subwindow_days or 1))
    start = _parse_iso(start_iso)
    end = _parse_iso(end_iso)
    delta = datetime.timedelta(days=days)
    windows = []
    cur = start
    while cur < end:
        nxt = min(cur + delta, end)
        windows.append((_to_iso(cur), _to_iso(nxt)))
        cur = nxt
    return windows
```

- [ ] **Step 4: Run to verify they pass**

Run: `pytest app/actions/tests/test_actions.py -k iter_subwindows -v`
Expected: PASS (5 tests).

- [ ] **Step 5: Commit**

```bash
git add app/actions/handlers.py app/actions/tests/test_actions.py
git commit -m "feat: add ISO-tolerant sub-window iteration helpers"
```

---

## Task 4: New config fields `subwindow_days` and `continue_immediately`

**Files:**
- Modify: `app/actions/configurations.py:117-176` (`PullObservationsConfig`)
- Test: `app/actions/tests/test_actions.py`

- [ ] **Step 1: Write the failing test**

Add to `app/actions/tests/test_actions.py`:

```python
def test_pull_observations_config_backfill_defaults():
    from app.actions.configurations import PullObservationsConfig
    cfg = PullObservationsConfig(start_datetime="2025-01-01T00:00:00+00:00")
    assert cfg.subwindow_days == 1
    assert cfg.continue_immediately is False
```

- [ ] **Step 2: Run to verify it fails**

Run: `pytest app/actions/tests/test_actions.py::test_pull_observations_config_backfill_defaults -v`
Expected: FAIL with `AttributeError: 'PullObservationsConfig' object has no attribute 'subwindow_days'`.

- [ ] **Step 3: Implement**

In `app/actions/configurations.py`, add these two fields to `PullObservationsConfig` immediately after the `subject_group_ids` field (before `run_on_schedule`):

```python
    subwindow_days: int = FieldWithUIOptions(
        1,
        title="Sub-window Size (days)",
        description=(
            "Backfill granularity: the pull processes the [start, end] window in "
            "slices this many days wide, per source, committing progress after each "
            "slice. Smaller values bound memory and re-work on resume; larger values "
            "reduce per-slice overhead. Default 1."
        ),
        ge=1,
        ui_options=UIOptions(widget="updown"),
    )
    continue_immediately: bool = FieldWithUIOptions(
        False,
        title="Continue Immediately (self-re-trigger)",
        description=(
            "When a run hits its time budget with work remaining, immediately "
            "re-trigger the next chunk via PubSub instead of waiting for the next "
            "scheduled tick. Faster backfills, but requires INTEGRATION_COMMANDS_TOPIC "
            "to be configured. Off by default (scheduler-driven catch-up)."
        ),
    )
```

Then update the `ui_global_options` order list for `PullObservationsConfig` to include the new fields:

```python
    ui_global_options: GlobalUISchemaOptions = GlobalUISchemaOptions(
        order=["start_datetime", "end_datetime", "subject_group_ids", "subwindow_days", "force_run_since_start", "continue_immediately", "run_on_schedule"],
    )
```

- [ ] **Step 4: Run to verify it passes**

Run: `pytest app/actions/tests/test_actions.py::test_pull_observations_config_backfill_defaults -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add app/actions/configurations.py app/actions/tests/test_actions.py
git commit -m "feat: add subwindow_days and continue_immediately to PullObservationsConfig"
```

---

## Task 5: Chunked source-assignment fetch with pagination warning + diagnostics

Fixes problem (a) (giant URL + dropped pages) and adds problem (b) diagnostics. `_resolve_source_ids` is rewired to use the new helper; existing `_resolve_source_ids` tests must keep passing (they use <25 subjects → one chunk → one call).

**Files:**
- Modify: `app/actions/handlers.py` (`_resolve_source_ids` around lines 685-724; add `_fetch_source_assignments`)
- Test: `app/actions/tests/test_actions.py`

- [ ] **Step 1: Write the failing tests**

Add to `app/actions/tests/test_actions.py`:

```python
@pytest.mark.asyncio
async def test_fetch_source_assignments_chunks_subject_ids(mocker):
    """Subjects are chunked into SUBJECT_ID_CHUNK_SIZE-sized requests and aggregated."""
    from app.actions.handlers import _fetch_source_assignments

    calls = []

    async def fake_get_source_assignments(subject_ids=None, source_ids=None):
        calls.append(list(subject_ids))
        return [{"subject": s, "source": f"src-{s}"} for s in subject_ids]

    er_client = mocker.MagicMock()
    er_client.get_source_assignments.side_effect = fake_get_source_assignments

    subject_ids = [f"subj-{i}" for i in range(60)]  # > 2 chunks of 25
    assignments = await _fetch_source_assignments(er_client, subject_ids)

    assert [len(c) for c in calls] == [25, 25, 10]
    assert len(assignments) == 60


@pytest.mark.asyncio
async def test_fetch_source_assignments_warns_on_paginated_next(mocker, caplog):
    """A chunk whose envelope carries a 'next' is flagged loudly (data may be dropped)."""
    import logging
    from app.actions.handlers import _fetch_source_assignments

    async def fake_get_source_assignments(subject_ids=None, source_ids=None):
        return {"count": 200, "next": "http://er/subjectsources?cursor=abc",
                "previous": None, "results": [{"subject": "s", "source": "src-1"}]}

    er_client = mocker.MagicMock()
    er_client.get_source_assignments.side_effect = fake_get_source_assignments

    with caplog.at_level(logging.WARNING):
        assignments = await _fetch_source_assignments(er_client, ["s"])

    assert assignments == [{"subject": "s", "source": "src-1"}]
    assert any("paginated" in r.message.lower() or "next" in r.message.lower()
               for r in caplog.records)


@pytest.mark.asyncio
async def test_fetch_source_assignments_skips_malformed_records(mocker, caplog):
    """Records that aren't dicts-with-source are skipped, not crashed on."""
    import logging
    from app.actions.handlers import _fetch_source_assignments

    async def fake_get_source_assignments(subject_ids=None, source_ids=None):
        return ["a-bare-string", {"subject": "s"}, {"subject": "s", "source": "src-ok"}]

    er_client = mocker.MagicMock()
    er_client.get_source_assignments.side_effect = fake_get_source_assignments

    with caplog.at_level(logging.WARNING):
        assignments = await _fetch_source_assignments(er_client, ["s"])

    assert assignments == [{"subject": "s", "source": "src-ok"}]
```

- [ ] **Step 2: Run to verify they fail**

Run: `pytest app/actions/tests/test_actions.py -k fetch_source_assignments -v`
Expected: FAIL with `ImportError: cannot import name '_fetch_source_assignments'`.

- [ ] **Step 3: Implement**

Add `_fetch_source_assignments` to `app/actions/handlers.py` (just before `_resolve_source_ids`):

```python
async def _fetch_source_assignments(er_client, subject_ids, *, integration_id=None):
    """Fetch subjectsources for many subjects, chunked to keep URLs short and
    each response within a single page.

    ER's ``subjectsources`` endpoint is a single (non-paginated) request: a huge
    ``subjects=`` query string risks a 414, and a response large enough to
    paginate would be silently truncated. Chunking subjects into small groups
    sidesteps both. As a safety net we WARN if any chunk response still carries a
    ``next`` (the single-page assumption breaking), and we capture diagnostics on
    unexpected/malformed shapes (problem (b)).
    """
    assignments = []
    malformed = 0
    for chunk in _chunked(subject_ids, SUBJECT_ID_CHUNK_SIZE):
        raw = await er_client.get_source_assignments(subject_ids=chunk)
        if isinstance(raw, dict):
            if raw.get("next"):
                logger.warning(
                    "subjectsources chunk returned a paginated 'next' "
                    "(count=%s, chunk_size=%d) — sources may be dropped; "
                    "lower SUBJECT_ID_CHUNK_SIZE.",
                    raw.get("count"), len(chunk),
                    extra={"attention_needed": True},
                )
            records = raw.get("results", [])
        elif isinstance(raw, list):
            records = raw
        else:
            logger.warning(
                "Unexpected subjectsources response type %s: %r",
                type(raw).__name__, raw,
                extra={"attention_needed": True},
            )
            records = []
        if records:
            logger.debug(
                "subjectsources chunk: response_type=%s records=%d sample=%r",
                type(raw).__name__, len(records), records[0],
            )
        for rec in records:
            if isinstance(rec, dict) and rec.get("source"):
                assignments.append(rec)
            else:
                malformed += 1
                logger.warning(
                    "Skipping malformed subjectsource record: %r", rec,
                    extra={"attention_needed": True},
                )
    if malformed and integration_id:
        await log_action_activity(
            integration_id=integration_id,
            action_id="pull_observations",
            title="Some source-assignment records were malformed and skipped",
            level=LogLevel.WARNING,
            data={"malformed_count": malformed},
        )
    return assignments
```

Then replace the body of `_resolve_source_ids` (the part from `assignments = await er_client.get_source_assignments(...)` to the end) so it delegates to the new helper. The full updated function:

```python
async def _resolve_source_ids(er_client, group_ids, *, integration_id=None):
    """Resolve subject-group UUIDs to a set of source UUIDs.

    Walks ER's subjectgroup tree recursively (flat=False). When a matched UUID
    is found, every descendant subject is included. Then resolves the subjects'
    current source assignments via ``_fetch_source_assignments`` (chunked, so a
    large subject list neither overruns the URL nor drops paginated rows).
    """
    if not group_ids:
        return set()

    wanted = set(group_ids)
    groups = await er_client.get_subjectgroups(flat=False)
    subject_ids = set()

    def walk(group, inherited=False):
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

    assignments = await _fetch_source_assignments(
        er_client, sorted(subject_ids), integration_id=integration_id
    )
    return {str(a["source"]) for a in assignments if a.get("source")}
```

Note: the old `_as_list`-based body is removed. `_as_list` is still used elsewhere (`action_show_permissions`, `_fetch_event_type_maps`) so leave it defined.

- [ ] **Step 4: Run to verify the new and existing assignment tests pass**

Run: `pytest app/actions/tests/test_actions.py -k "source_assignments or resolve_source_ids" -v`
Expected: PASS (3 new + 4 pre-existing `_resolve_source_ids` tests).

- [ ] **Step 5: Commit**

```bash
git add app/actions/handlers.py app/actions/tests/test_actions.py
git commit -m "feat: chunk subjectsources fetch with pagination warning and diagnostics"
```

---

## Task 6: Backfill lease (overlap guard)

**Files:**
- Modify: `app/actions/handlers.py` (add constants + two helpers; add `from app import settings`)
- Test: `app/actions/tests/test_actions.py`

- [ ] **Step 1: Write the failing tests**

Add to `app/actions/tests/test_actions.py`:

```python
@pytest.mark.asyncio
async def test_acquire_backfill_lease_returns_false_when_held(mocker):
    from app.actions.handlers import _acquire_backfill_lease, BACKFILL_LOCK_SOURCE_ID
    sm = mocker.patch("app.actions.handlers.state_manager")
    sm.set_if_absent.return_value = async_return_local(False)
    got = await _acquire_backfill_lease("int-1")
    assert got is False
    assert sm.set_if_absent.call_args.kwargs["source_id"] == BACKFILL_LOCK_SOURCE_ID


@pytest.mark.asyncio
async def test_acquire_backfill_lease_fails_open_on_redis_error(mocker):
    from app.actions.handlers import _acquire_backfill_lease
    sm = mocker.patch("app.actions.handlers.state_manager")
    sm.set_if_absent.side_effect = RuntimeError("redis down")
    # Fail open: a missing lease is a rare duplicate, not a crash.
    assert await _acquire_backfill_lease("int-1") is True


@pytest.mark.asyncio
async def test_release_backfill_lease_deletes_lock_key(mocker):
    from app.actions.handlers import _release_backfill_lease, BACKFILL_LOCK_SOURCE_ID
    sm = mocker.patch("app.actions.handlers.state_manager")
    sm.delete_state.return_value = async_return_local(None)
    await _release_backfill_lease("int-1")
    assert sm.delete_state.call_args.kwargs["source_id"] == BACKFILL_LOCK_SOURCE_ID
```

Add this small awaitable helper at the top of `app/actions/tests/test_actions.py` (after the imports) if not already present, so these unit tests don't depend on the conftest fixture:

```python
import asyncio as _asyncio

def async_return_local(result):
    f = _asyncio.Future()
    f.set_result(result)
    return f
```

- [ ] **Step 2: Run to verify they fail**

Run: `pytest app/actions/tests/test_actions.py -k backfill_lease -v`
Expected: FAIL with `ImportError: cannot import name '_acquire_backfill_lease'`.

- [ ] **Step 3: Implement**

Add `from app import settings` to the imports in `app/actions/handlers.py` if not present. Add the constants from the File Structure section near the top (after `BATCH_SIZE`). Then add:

```python
async def _acquire_backfill_lease(integration_id):
    """Acquire the per-(integration, pull_observations) lease.

    Returns True if this invocation may proceed, False if another invocation
    currently holds it. The TTL is the crash backstop: because the handler is
    hard-killed by asyncio.wait_for at MAX_ACTION_EXECUTION_TIME, a run can never
    outlive its own lease. Fails OPEN on a state-store error (a rare duplicate is
    cheaper than turning a benign no-op into a crash).
    """
    ttl = int(settings.MAX_ACTION_EXECUTION_TIME) + LOCK_MARGIN_SECONDS
    try:
        return await state_manager.set_if_absent(
            integration_id=integration_id,
            action_id="pull_observations",
            source_id=BACKFILL_LOCK_SOURCE_ID,
            ttl_seconds=ttl,
        )
    except Exception as e:
        logger.warning(
            "Backfill lease acquire failed (%s); proceeding without lease.", e
        )
        return True


async def _release_backfill_lease(integration_id):
    """Release the lease. Best-effort: if this fails, the TTL expires it."""
    try:
        await state_manager.delete_state(
            integration_id=integration_id,
            action_id="pull_observations",
            source_id=BACKFILL_LOCK_SOURCE_ID,
        )
    except Exception as e:
        logger.warning(
            "Backfill lease release failed (%s); TTL will expire it.", e
        )
```

- [ ] **Step 4: Run to verify they pass**

Run: `pytest app/actions/tests/test_actions.py -k backfill_lease -v`
Expected: PASS (3 tests).

- [ ] **Step 5: Commit**

```bash
git add app/actions/handlers.py app/actions/tests/test_actions.py
git commit -m "feat: add backfill lease acquire/release helpers"
```

---

## Task 7: Cursor build/save + single-unit drain helpers

**Files:**
- Modify: `app/actions/handlers.py` (add `_build_backfill_cursor`, `_save_backfill_cursor`, `_pull_source_window`)
- Test: `app/actions/tests/test_actions.py`

- [ ] **Step 1: Write the failing tests**

Add to `app/actions/tests/test_actions.py`:

```python
def test_build_backfill_cursor_with_sources():
    from app.actions.handlers import _build_backfill_cursor
    cursor = _build_backfill_cursor(
        start="2025-01-01T00:00:00+00:00",
        end="2025-01-03T00:00:00+00:00",
        subwindow_days=1,
        source_ids={"src-b", "src-a"},
    )
    assert cursor == {
        "start": "2025-01-01T00:00:00+00:00",
        "end": "2025-01-03T00:00:00+00:00",
        "subwindow_days": 1,
        "sources": ["src-a", "src-b"],  # sorted, deterministic
        "window_index": 0,
        "source_index": 0,
        "no_progress_count": 0,
    }


def test_build_backfill_cursor_without_sources_uses_none_sentinel():
    from app.actions.handlers import _build_backfill_cursor
    cursor = _build_backfill_cursor(
        start="2025-01-01T00:00:00+00:00",
        end="2025-01-02T00:00:00+00:00",
        subwindow_days=1,
        source_ids=set(),
    )
    # No group filter → one synthetic source (None) = whole-instance per window.
    assert cursor["sources"] == [None]


@pytest.mark.asyncio
async def test_save_backfill_cursor_preserves_last_execution(mocker):
    from app.actions.handlers import _save_backfill_cursor
    sm = mocker.patch("app.actions.handlers.state_manager")
    sm.set_state.return_value = async_return_local(None)
    cursor = {"window_index": 1, "source_index": 0}
    await _save_backfill_cursor("int-1", last_execution="2023-01-01T00:00:00+00:00", cursor=cursor)
    saved = sm.set_state.call_args.kwargs["state"]
    assert saved == {"last_execution": "2023-01-01T00:00:00+00:00", "backfill": cursor}


@pytest.mark.asyncio
async def test_pull_source_window_passes_source_and_window_to_er(mocker):
    from app.actions.handlers import _pull_source_window
    from app.actions.tests.conftest import AsyncIterator

    er_client = mocker.MagicMock()
    er_client.get_observations.return_value = AsyncIterator([[
        {"id": "o1", "source": "src-1", "recorded_at": "2025-01-01T00:00:00Z"},
    ]])
    sent = mocker.patch("app.actions.handlers.send_observations_to_gundi")
    sent.return_value = async_return_local(None)

    count = await _pull_source_window(
        er_client, "src-1", "2025-01-01T00:00:00+00:00",
        "2025-01-02T00:00:00+00:00", integration_id="int-1",
    )

    assert count == 1
    kwargs = er_client.get_observations.call_args.kwargs
    assert kwargs["source_id"] == "src-1"
    assert kwargs["start"] == "2025-01-01T00:00:00+00:00"
    assert kwargs["end"] == "2025-01-02T00:00:00+00:00"


@pytest.mark.asyncio
async def test_pull_source_window_none_source_sends_no_source_id(mocker):
    from app.actions.handlers import _pull_source_window
    from app.actions.tests.conftest import AsyncIterator

    er_client = mocker.MagicMock()
    er_client.get_observations.return_value = AsyncIterator([[
        {"id": "o1", "source": "src-x", "recorded_at": "2025-01-01T00:00:00Z"},
    ]])
    sent = mocker.patch("app.actions.handlers.send_observations_to_gundi")
    sent.return_value = async_return_local(None)

    await _pull_source_window(
        er_client, None, "2025-01-01T00:00:00+00:00",
        "2025-01-02T00:00:00+00:00", integration_id="int-1",
    )

    assert "source_id" not in er_client.get_observations.call_args.kwargs
```

- [ ] **Step 2: Run to verify they fail**

Run: `pytest app/actions/tests/test_actions.py -k "backfill_cursor or pull_source_window" -v`
Expected: FAIL with `ImportError`.

- [ ] **Step 3: Implement**

Add to `app/actions/handlers.py`:

```python
def _build_backfill_cursor(*, start, end, subwindow_days, source_ids):
    """Snapshot the work definition + zeroed progress for a new backfill run.

    ``source_ids`` is snapshotted (sorted) so the unit sequence is stable across
    resumes. An empty set means "no group filter" → a single ``None`` source,
    i.e. one whole-instance fetch per sub-window.
    """
    sources = sorted(source_ids) if source_ids else [None]
    return {
        "start": start,
        "end": end,
        "subwindow_days": int(subwindow_days or 1),
        "sources": sources,
        "window_index": 0,
        "source_index": 0,
        "no_progress_count": 0,
    }


async def _save_backfill_cursor(integration_id, *, last_execution, cursor):
    """Persist the cursor alongside the (unchanged) watermark.

    The watermark is only advanced on completion; until then it is preserved so
    a failure never loses the previously-confirmed window.
    """
    state = {"backfill": cursor}
    if last_execution is not None:
        state["last_execution"] = last_execution
    await state_manager.set_state(
        integration_id=integration_id,
        action_id="pull_observations",
        state=state,
    )


async def _pull_source_window(er_client, source, start, end, *, integration_id):
    """Drain one (source × sub-window) unit and forward to Gundi.

    ``source=None`` means no source filter (whole instance for the window).
    Returns the number of observations forwarded. ER filters server-side, so no
    client-side source filtering is needed.
    """
    params = {"start": start, "end": end, "batch_size": BATCH_SIZE}
    if source is not None:
        params["source_id"] = source
    sent = 0
    async for observation_batch in er_client.get_observations(**params):
        transformed = transform_observations_to_gundi_schema(observations=observation_batch)
        if not transformed:
            continue
        logger.info(f"Sending {len(transformed)} observations to Gundi...")
        await send_observations_to_gundi(observations=transformed, integration_id=integration_id)
        sent += len(transformed)
    return sent
```

- [ ] **Step 4: Run to verify they pass**

Run: `pytest app/actions/tests/test_actions.py -k "backfill_cursor or pull_source_window" -v`
Expected: PASS (5 tests).

- [ ] **Step 5: Commit**

```bash
git add app/actions/handlers.py app/actions/tests/test_actions.py
git commit -m "feat: add backfill cursor and single-unit drain helpers"
```

---

## Task 8: Rewrite `action_pull_observations` orchestration

This wires together per-source server-side filtering, sub-window chunking, the cursor, the soft time budget, the lease, and completion. Self-re-trigger is added in Task 9. Three existing e2e tests change contract (they gain `"status"` and, for the per-source case, assert per-source calls); new e2e tests cover resume / budget / overlap / completion.

**Files:**
- Modify: `app/actions/handlers.py:594-682` (replace `action_pull_observations` body)
- Modify: `app/actions/tests/test_actions.py` (update 3 existing tests, add 4 new)

- [ ] **Step 1: Replace the handler**

Replace the entire `action_pull_observations` function (lines 594-682) with:

```python
@activity_logger()
async def action_pull_observations(integration: Integration, action_config: PullObservationsConfig):
    integration_id = str(integration.id)
    logger.info(
        f"Extracting observations for integration {integration_id}, with config {action_config}",
    )
    execution_timestamp = datetime.datetime.now(tz=datetime.timezone.utc).isoformat()
    pull_config = action_config
    auth_config = get_authentication_config(integration=integration)
    url_parse = urlparse(integration.base_url)
    er_client = AsyncERClient(
        service_root=f"{url_parse.scheme}://{url_parse.hostname}/api/v1.0",
        username=auth_config.username or None,
        password=auth_config.password.get_secret_value() if auth_config.password else None,
        token=auth_config.token.get_secret_value() if auth_config.token else None,
        token_url=f"{url_parse.scheme}://{url_parse.hostname}/oauth2/token",
        client_id="das_web_client",
        connect_timeout=DEFAULT_CONNECT_TIMEOUT_SECONDS,
    )

    start_monotonic = time.monotonic()
    soft_budget = settings.MAX_ACTION_EXECUTION_TIME * BUDGET_FRACTION

    async with er_client as earth_ranger:
        # Mutual exclusion: a long backfill may still be running when the next
        # scheduled tick fires. Without the lease, both would process the same
        # cursor units concurrently (duplicate sends + cursor races).
        if not await _acquire_backfill_lease(integration_id):
            return _skip_quietly(
                integration_id, "pull_observations",
                reason="backfill_in_progress",
                message="Skipping 'pull_observations': another run holds the backfill lease.",
                log_level=logging.INFO,
            )
        try:
            state = await state_manager.get_state(
                integration_id=integration_id, action_id="pull_observations"
            )
            cursor = state.get("backfill")
            last_execution = state.get("last_execution")

            if cursor is None:
                # Fresh run: compute the window and resolve the source list once.
                last = state.get("last_execution")
                if not last or pull_config.force_run_since_start:
                    window_start = pull_config.start_datetime
                else:
                    window_start = last
                window_end = pull_config.end_datetime or execution_timestamp

                source_id_set = await _resolve_source_ids(
                    earth_ranger,
                    group_ids=pull_config.subject_group_ids,
                    integration_id=integration_id,
                )
                if pull_config.subject_group_ids and not source_id_set:
                    await log_action_activity(
                        integration_id=integration_id,
                        action_id="pull_observations",
                        title="Configured subject groups resolved to zero active sources",
                        level=LogLevel.ERROR,
                        data={"subject_group_ids": pull_config.subject_group_ids},
                    )
                    # Watermark intentionally NOT advanced; operator fix re-pulls.
                    return {
                        "status": "skipped_no_sources",
                        "observations_extracted": 0,
                        "filter_active": True,
                        "sources_resolved": 0,
                    }
                cursor = _build_backfill_cursor(
                    start=window_start,
                    end=window_end,
                    subwindow_days=pull_config.subwindow_days,
                    source_ids=source_id_set,
                )

            filter_active = cursor["sources"] != [None]
            subwindows = _iter_subwindows(
                cursor["start"], cursor["end"], cursor["subwindow_days"]
            )

            total_observations = 0
            units_completed = 0
            wi = cursor["window_index"]
            si = cursor["source_index"]

            while wi < len(subwindows):
                w_start, w_end = subwindows[wi]
                while si < len(cursor["sources"]):
                    # Yield before starting a unit if the soft budget is spent.
                    if time.monotonic() - start_monotonic >= soft_budget:
                        cursor["window_index"] = wi
                        cursor["source_index"] = si
                        cursor["no_progress_count"] = (
                            cursor.get("no_progress_count", 0) + 1
                            if units_completed == 0 else 0
                        )
                        await _save_backfill_cursor(
                            integration_id, last_execution=last_execution, cursor=cursor
                        )
                        logger.info(
                            "pull_observations yielding (budget): window %d/%d source %d/%d",
                            wi, len(subwindows), si, len(cursor["sources"]),
                        )
                        return {
                            "status": "in_progress",
                            "observations_extracted": total_observations,
                            "window_index": wi,
                            "source_index": si,
                            "filter_active": filter_active,
                            "sources_resolved": len(cursor["sources"]) if filter_active else None,
                        }
                    source = cursor["sources"][si]
                    try:
                        total_observations += await _pull_source_window(
                            earth_ranger, source, w_start, w_end,
                            integration_id=integration_id,
                        )
                    except Exception as e:
                        # Don't wedge the backfill on one bad unit: log loudly and
                        # advance past it (at-least-once; operator can re-pull).
                        logger.error(
                            "pull_observations unit failed (source=%r window=%s..%s): %s",
                            source, w_start, w_end, e,
                            extra={"attention_needed": True},
                        )
                    si += 1
                    units_completed += 1
                    cursor["window_index"] = wi
                    cursor["source_index"] = si
                    await _save_backfill_cursor(
                        integration_id, last_execution=last_execution, cursor=cursor
                    )
                si = 0
                wi += 1

            # All units done → advance the watermark to the window end and clear
            # the cursor (drops "backfill", sets last_execution).
            await state_manager.set_state(
                integration_id=integration_id,
                action_id="pull_observations",
                state={"last_execution": cursor["end"]},
            )
            logger.info(
                f"Extracted {total_observations} observations for integration {integration}."
            )
            return {
                "status": "complete",
                "observations_extracted": total_observations,
                "filter_active": filter_active,
                "sources_resolved": len(cursor["sources"]) if filter_active else None,
            }
        finally:
            await _release_backfill_lease(integration_id)
```

Add `import time` to the top of `app/actions/handlers.py` if not already imported.

- [ ] **Step 2: Update the no-filter e2e test**

Replace the assertion block in `test_execute_pull_observations_action` (lines 138-142) with:

```python
    assert response == {
        "status": "complete",
        "observations_extracted": len(observations_batch_one) + len(observations_batch_two),
        "filter_active": False,
        "sources_resolved": None,
    }
```

(The single shared `get_observations` mock iterator is consumed by the first sub-window's `None`-source fetch, yielding both batches → `post_observations.call_count == 2` still holds. Later sub-windows receive the exhausted iterator and yield nothing.)

- [ ] **Step 3: Update the per-source filter e2e test**

In `test_pull_observations_filters_by_resolved_source_ids` (around lines 661-722), ER now filters server-side per source, so replace the single mixed-batch mock and the assertions. Replace the `get_observations` setup (lines 692-697) with a per-source `side_effect`:

```python
    from app.actions.tests.conftest import AsyncIterator
    per_source = {
        "src-keep-1": [[{"id": "obs-1", "source": "src-keep-1", "recorded_at": "2025-01-01T00:00:00Z"}]],
        "src-keep-2": [[{"id": "obs-3", "source": "src-keep-2", "recorded_at": "2025-01-01T00:00:02Z"}]],
    }

    def fake_get_observations(**kwargs):
        return AsyncIterator(per_source.get(kwargs.get("source_id"), []))

    mock_erclient_class.return_value.get_observations.side_effect = fake_get_observations
```

And replace the response assertion (lines 714-722) with:

```python
    assert response == {
        "status": "complete",
        "observations_extracted": 2,
        "filter_active": True,
        "sources_resolved": 2,
    }
    forwarded_sources = set()
    for call in mock_gundi_sensors_client_class.return_value.post_observations.call_args_list:
        for o in call.kwargs["data"]:
            forwarded_sources.add(o["source"])
    assert forwarded_sources == {"er-src-src-keep-1", "er-src-src-keep-2"}
```

- [ ] **Step 4: Update the zero-resolved-sources e2e test**

Find the test asserting the zero-source shape (lines ~761-763: `observations_extracted: 0, filter_active: True, sources_resolved: 0`) and update its expected dict to include the status key:

```python
    assert response == {
        "status": "skipped_no_sources",
        "observations_extracted": 0,
        "filter_active": True,
        "sources_resolved": 0,
    }
```

- [ ] **Step 5: Run the updated existing tests**

Run: `pytest app/actions/tests/test_actions.py -k "pull_observations" -v`
Expected: PASS (the 3 updated e2e tests). Fix any mock wiring before continuing.

- [ ] **Step 6: Add new e2e tests (resume, budget, overlap, completion)**

Add to `app/actions/tests/test_actions.py`:

```python
@pytest.mark.asyncio
async def test_pull_observations_resumes_from_existing_cursor(
        mocker, mock_gundi_client_v2, mock_state_manager, mock_erclient_class,
        mock_get_gundi_api_key, mock_gundi_sensors_client_class, er_integration_v2_provider,
        mock_publish_event, mock_gundi_client_v2_class, mock_config_manager_er_provider
):
    """A live cursor is authoritative: the run resumes from it and does NOT
    re-resolve sources."""
    cursor = {
        "start": "2025-01-01T00:00:00+00:00",
        "end": "2025-01-02T00:00:00+00:00",
        "subwindow_days": 1,
        "sources": ["src-a"],
        "window_index": 0,
        "source_index": 0,
        "no_progress_count": 0,
    }
    mock_state_manager.get_state.return_value = async_return_local({
        "last_execution": "2024-12-01T00:00:00+00:00",
        "backfill": cursor,
    })

    from app.actions.tests.conftest import AsyncIterator
    mock_erclient_class.return_value.get_observations.side_effect = (
        lambda **kw: AsyncIterator([[{"id": "o1", "source": "src-a", "recorded_at": "2025-01-01T01:00:00Z"}]])
    )

    mocker.patch("app.services.action_runner._portal", mock_gundi_client_v2)
    mocker.patch("app.services.activity_logger.publish_event", mock_publish_event)
    mocker.patch("app.services.action_runner.publish_event", mock_publish_event)
    mocker.patch("app.services.action_runner.config_manager", mock_config_manager_er_provider)
    mocker.patch("app.actions.handlers.state_manager", mock_state_manager)
    mocker.patch("app.actions.handlers.AsyncERClient", mock_erclient_class)
    mocker.patch("app.services.gundi.GundiClient", mock_gundi_client_v2_class)
    mocker.patch("app.services.gundi.GundiDataSenderClient", mock_gundi_sensors_client_class)
    mocker.patch("app.services.gundi._get_gundi_api_key", mock_get_gundi_api_key)

    response = await execute_action(
        integration_id=str(er_integration_v2_provider.id),
        action_id="pull_observations",
    )

    assert response["status"] == "complete"
    # Resumed run never resolves sources from groups.
    mock_erclient_class.return_value.get_subjectgroups.assert_not_called()
    # Final set_state advances the watermark to the window end.
    final = mock_state_manager.set_state.call_args.kwargs["state"]
    assert final == {"last_execution": "2025-01-02T00:00:00+00:00"}


@pytest.mark.asyncio
async def test_pull_observations_yields_in_progress_when_budget_exceeded(
        mocker, mock_gundi_client_v2, mock_state_manager, mock_erclient_class,
        mock_get_gundi_api_key, mock_gundi_sensors_client_class, er_integration_v2_provider,
        mock_publish_event, mock_gundi_client_v2_class, mock_config_manager_er_provider
):
    """When the soft budget is already spent at the first unit, the run commits a
    cursor and returns in_progress without fetching observations."""
    cursor = {
        "start": "2025-01-01T00:00:00+00:00",
        "end": "2025-01-05T00:00:00+00:00",
        "subwindow_days": 1,
        "sources": ["src-a"],
        "window_index": 0,
        "source_index": 0,
        "no_progress_count": 0,
    }
    mock_state_manager.get_state.return_value = async_return_local({
        "last_execution": "2024-12-01T00:00:00+00:00",
        "backfill": cursor,
    })
    # Force "budget exceeded" immediately: monotonic jumps far past the budget.
    mocker.patch("app.actions.handlers.time.monotonic", side_effect=[0.0, 10**9])

    mocker.patch("app.services.action_runner._portal", mock_gundi_client_v2)
    mocker.patch("app.services.activity_logger.publish_event", mock_publish_event)
    mocker.patch("app.services.action_runner.publish_event", mock_publish_event)
    mocker.patch("app.services.action_runner.config_manager", mock_config_manager_er_provider)
    mocker.patch("app.actions.handlers.state_manager", mock_state_manager)
    mocker.patch("app.actions.handlers.AsyncERClient", mock_erclient_class)
    mocker.patch("app.services.gundi.GundiClient", mock_gundi_client_v2_class)
    mocker.patch("app.services.gundi.GundiDataSenderClient", mock_gundi_sensors_client_class)
    mocker.patch("app.services.gundi._get_gundi_api_key", mock_get_gundi_api_key)

    response = await execute_action(
        integration_id=str(er_integration_v2_provider.id),
        action_id="pull_observations",
    )

    assert response["status"] == "in_progress"
    assert response["window_index"] == 0 and response["source_index"] == 0
    mock_erclient_class.return_value.get_observations.assert_not_called()
    # Cursor was persisted (set_state called with a "backfill" key).
    saved = mock_state_manager.set_state.call_args.kwargs["state"]
    assert "backfill" in saved


@pytest.mark.asyncio
async def test_pull_observations_skips_when_lease_held(
        mocker, mock_gundi_client_v2, mock_state_manager, mock_erclient_class,
        mock_get_gundi_api_key, mock_gundi_sensors_client_class, er_integration_v2_provider,
        mock_publish_event, mock_gundi_client_v2_class, mock_config_manager_er_provider
):
    """An overlapping invocation (lease already held) is a clean no-op: no cursor
    read, no ER calls."""
    mock_state_manager.set_if_absent.return_value = async_return_local(False)

    mocker.patch("app.services.action_runner._portal", mock_gundi_client_v2)
    mocker.patch("app.services.activity_logger.publish_event", mock_publish_event)
    mocker.patch("app.services.action_runner.publish_event", mock_publish_event)
    mocker.patch("app.services.action_runner.config_manager", mock_config_manager_er_provider)
    mocker.patch("app.actions.handlers.state_manager", mock_state_manager)
    mocker.patch("app.actions.handlers.AsyncERClient", mock_erclient_class)
    mocker.patch("app.services.gundi.GundiClient", mock_gundi_client_v2_class)
    mocker.patch("app.services.gundi.GundiDataSenderClient", mock_gundi_sensors_client_class)
    mocker.patch("app.services.gundi._get_gundi_api_key", mock_get_gundi_api_key)

    response = await execute_action(
        integration_id=str(er_integration_v2_provider.id),
        action_id="pull_observations",
    )

    assert response == {"skipped": True, "reason": "backfill_in_progress"}
    mock_erclient_class.return_value.get_observations.assert_not_called()
    mock_state_manager.get_state.assert_not_called()
```

- [ ] **Step 7: Run the new e2e tests**

Run: `pytest app/actions/tests/test_actions.py -k "resumes_from_existing_cursor or yields_in_progress or skips_when_lease_held" -v`
Expected: PASS (3 tests).

- [ ] **Step 8: Commit**

```bash
git add app/actions/handlers.py app/actions/tests/test_actions.py
git commit -m "feat: per-source, chunked, resumable, lease-guarded pull_observations"
```

---

## Task 9: Opt-in self-re-trigger with no-progress guard

**Files:**
- Modify: `app/actions/handlers.py` (the `in_progress` return path in `action_pull_observations`)
- Test: `app/actions/tests/test_actions.py`

- [ ] **Step 1: Write the failing tests**

Add to `app/actions/tests/test_actions.py`:

```python
@pytest.mark.asyncio
async def test_pull_observations_self_retriggers_when_enabled(
        mocker, mock_gundi_client_v2, mock_state_manager, mock_erclient_class,
        mock_get_gundi_api_key, mock_gundi_sensors_client_class, er_integration_v2_provider,
        mock_publish_event, mock_gundi_client_v2_class, mock_config_manager_er_provider
):
    """With continue_immediately on, an in_progress run re-triggers the next chunk."""
    pull_obs_data = er_integration_v2_provider.get_action_config("pull_observations").data
    pull_obs_data["continue_immediately"] = True

    cursor = {
        "start": "2025-01-01T00:00:00+00:00", "end": "2025-01-05T00:00:00+00:00",
        "subwindow_days": 1, "sources": ["src-a"],
        "window_index": 0, "source_index": 0, "no_progress_count": 0,
    }
    mock_state_manager.get_state.return_value = async_return_local({
        "last_execution": "2024-12-01T00:00:00+00:00", "backfill": cursor,
    })
    # First unit runs (monotonic 0,0), then budget exceeded before the 2nd window.
    mocker.patch("app.actions.handlers.time.monotonic", side_effect=[0.0, 0.0, 10**9])
    from app.actions.tests.conftest import AsyncIterator
    mock_erclient_class.return_value.get_observations.side_effect = (
        lambda **kw: AsyncIterator([[{"id": "o1", "source": "src-a", "recorded_at": "2025-01-01T01:00:00Z"}]])
    )
    mock_trigger = mocker.patch("app.actions.handlers.trigger_action")
    mock_trigger.return_value = async_return_local(None)

    mocker.patch("app.services.action_runner._portal", mock_gundi_client_v2)
    mocker.patch("app.services.activity_logger.publish_event", mock_publish_event)
    mocker.patch("app.services.action_runner.publish_event", mock_publish_event)
    mocker.patch("app.services.action_runner.config_manager", mock_config_manager_er_provider)
    mocker.patch("app.actions.handlers.state_manager", mock_state_manager)
    mocker.patch("app.actions.handlers.AsyncERClient", mock_erclient_class)
    mocker.patch("app.services.gundi.GundiClient", mock_gundi_client_v2_class)
    mocker.patch("app.services.gundi.GundiDataSenderClient", mock_gundi_sensors_client_class)
    mocker.patch("app.services.gundi._get_gundi_api_key", mock_get_gundi_api_key)

    response = await execute_action(
        integration_id=str(er_integration_v2_provider.id),
        action_id="pull_observations",
    )

    assert response["status"] == "in_progress"
    mock_trigger.assert_called_once()
    assert mock_trigger.call_args.kwargs.get("action_id") == "pull_observations" \
        or mock_trigger.call_args.args[1] == "pull_observations"


@pytest.mark.asyncio
async def test_pull_observations_self_retrigger_stops_after_no_progress_limit(
        mocker, mock_gundi_client_v2, mock_state_manager, mock_erclient_class,
        mock_get_gundi_api_key, mock_gundi_sensors_client_class, er_integration_v2_provider,
        mock_publish_event, mock_gundi_client_v2_class, mock_config_manager_er_provider
):
    """If consecutive runs make zero progress, self-re-trigger stops (runaway guard)."""
    pull_obs_data = er_integration_v2_provider.get_action_config("pull_observations").data
    pull_obs_data["continue_immediately"] = True

    from app.actions.handlers import MAX_NO_PROGRESS_RETRIES
    cursor = {
        "start": "2025-01-01T00:00:00+00:00", "end": "2025-01-05T00:00:00+00:00",
        "subwindow_days": 1, "sources": ["src-a"],
        "window_index": 0, "source_index": 0,
        "no_progress_count": MAX_NO_PROGRESS_RETRIES,  # already at the limit
    }
    mock_state_manager.get_state.return_value = async_return_local({
        "last_execution": "2024-12-01T00:00:00+00:00", "backfill": cursor,
    })
    mocker.patch("app.actions.handlers.time.monotonic", side_effect=[0.0, 10**9])
    mock_trigger = mocker.patch("app.actions.handlers.trigger_action")
    mock_trigger.return_value = async_return_local(None)

    mocker.patch("app.services.action_runner._portal", mock_gundi_client_v2)
    mocker.patch("app.services.activity_logger.publish_event", mock_publish_event)
    mocker.patch("app.services.action_runner.publish_event", mock_publish_event)
    mocker.patch("app.services.action_runner.config_manager", mock_config_manager_er_provider)
    mocker.patch("app.actions.handlers.state_manager", mock_state_manager)
    mocker.patch("app.actions.handlers.AsyncERClient", mock_erclient_class)
    mocker.patch("app.services.gundi.GundiClient", mock_gundi_client_v2_class)
    mocker.patch("app.services.gundi.GundiDataSenderClient", mock_gundi_sensors_client_class)
    mocker.patch("app.services.gundi._get_gundi_api_key", mock_get_gundi_api_key)

    response = await execute_action(
        integration_id=str(er_integration_v2_provider.id),
        action_id="pull_observations",
    )

    assert response["status"] == "in_progress"
    mock_trigger.assert_not_called()
```

- [ ] **Step 2: Run to verify they fail**

Run: `pytest app/actions/tests/test_actions.py -k "self_retrigger" -v`
Expected: FAIL (`trigger_action` not imported / not called as expected).

- [ ] **Step 3: Implement**

Add a lazy import + re-trigger logic in the `in_progress` return path of `action_pull_observations`. Add `from app.services.action_scheduler import trigger_action` to the imports at the top of `app/actions/handlers.py`. Then, in the budget-exceeded block, replace the `return {...}` with re-trigger-then-return:

```python
                    if time.monotonic() - start_monotonic >= soft_budget:
                        cursor["window_index"] = wi
                        cursor["source_index"] = si
                        cursor["no_progress_count"] = (
                            cursor.get("no_progress_count", 0) + 1
                            if units_completed == 0 else 0
                        )
                        await _save_backfill_cursor(
                            integration_id, last_execution=last_execution, cursor=cursor
                        )
                        logger.info(
                            "pull_observations yielding (budget): window %d/%d source %d/%d",
                            wi, len(subwindows), si, len(cursor["sources"]),
                        )
                        # Opt-in: immediately re-trigger the next chunk via PubSub,
                        # unless we're making no progress (runaway guard).
                        if pull_config.continue_immediately:
                            if cursor["no_progress_count"] < MAX_NO_PROGRESS_RETRIES:
                                await trigger_action(integration_id, "pull_observations")
                            else:
                                logger.warning(
                                    "pull_observations not re-triggering: %d consecutive "
                                    "no-progress runs (runaway guard).",
                                    cursor["no_progress_count"],
                                    extra={"attention_needed": True},
                                )
                        return {
                            "status": "in_progress",
                            "observations_extracted": total_observations,
                            "window_index": wi,
                            "source_index": si,
                            "filter_active": filter_active,
                            "sources_resolved": len(cursor["sources"]) if filter_active else None,
                        }
```

- [ ] **Step 4: Run to verify they pass**

Run: `pytest app/actions/tests/test_actions.py -k "self_retrigger" -v`
Expected: PASS (2 tests).

- [ ] **Step 5: Commit**

```bash
git add app/actions/handlers.py app/actions/tests/test_actions.py
git commit -m "feat: opt-in self-re-trigger for observation backfill with runaway guard"
```

---

## Task 10: Full-suite verification + docs

**Files:**
- Modify: `docs/superpowers/specs/2026-06-13-er-observations-resumable-backfill-design.md` (mark implemented)

- [ ] **Step 1: Run the full action test suite**

Run: `pytest app/actions/tests/ -v`
Expected: PASS (all action tests, including the updated e2e ones).

- [ ] **Step 2: Run the entire test suite**

Run: `pytest --tb=short`
Expected: PASS. Investigate and fix any regression before continuing.

- [ ] **Step 3: Mark the spec implemented**

In `docs/superpowers/specs/2026-06-13-er-observations-resumable-backfill-design.md`, change the `**Status:**` line to `Implemented` and add a one-line note pointing at this plan.

- [ ] **Step 4: Commit**

```bash
git add docs/superpowers/specs/2026-06-13-er-observations-resumable-backfill-design.md
git commit -m "docs: mark resumable observation backfill spec implemented"
```

---

## Self-Review

**Spec coverage:**
- (a) Pagination fix → Task 5 (`_fetch_source_assignments` chunking + `next` warning).
- (b) Assignment-parsing diagnostics → Task 5 (type/shape/sample/malformed logging + activity log).
- (c) Per-source server-side filtering → Task 7 (`_pull_source_window`) + Task 8 (handler loops sources).
- (c) Bounded/resumable chunking → Tasks 3, 7, 8 (sub-windows, cursor, budget yield, completion).
- Concurrency/overlap lease → Task 6 + Task 8.
- Scheduler-driven default + opt-in self-re-trigger + runaway guard → Task 9.
- Config (`subwindow_days`, `continue_immediately`) → Task 4.
- At-least-once / unit-boundary commits → Task 8 (cursor saved after each unit; watermark only on completion).
- `pull_events` out of scope → untouched. Helpers (`_iter_subwindows`, `_pull_source_window`) are reusable for a later events adoption.

**Placeholder scan:** No TBD/TODO; every code step shows complete code; every test step shows assertions and the exact run command.

**Type/name consistency:** `_chunked`, `_iter_subwindows`, `_parse_iso`/`_to_iso`, `_fetch_source_assignments`, `_acquire_backfill_lease`/`_release_backfill_lease`, `BACKFILL_LOCK_SOURCE_ID`, `_build_backfill_cursor`/`_save_backfill_cursor`, `_pull_source_window`, and the cursor keys (`start`, `end`, `subwindow_days`, `sources`, `window_index`, `source_index`, `no_progress_count`) are used identically across tasks. Return shapes include `status` consistently (`complete` / `in_progress` / `skipped_no_sources`, plus the lease skip `{"skipped": True, "reason": "backfill_in_progress"}`). `async_return_local` is defined once in Task 6 and reused.

**Note for the executing engineer:** Tasks 2-7 are independent helpers and can be built in any order; Task 8 depends on Tasks 3, 5, 6, 7; Task 9 depends on Task 8; Task 1 (fixture) should land before Task 8's e2e tests run.
