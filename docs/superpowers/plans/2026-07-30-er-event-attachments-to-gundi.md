# ER Event Attachments → Gundi Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** During `pull_events`, read files attached to EarthRanger events (photos, documents) and forward them to Gundi as event attachments, so downstream destinations (CMORE) can deliver them.

**Architecture:** ER's events-list endpoint already includes a `files` array per event by default (each entry has `id`, `filename`, `url` — an authorized per-file download endpoint — and `file_type`). We add a `_forward_event_files()` helper that downloads each not-yet-seen file via `AsyncERClient.get_file(url)` and posts it with the existing `send_event_attachments_to_gundi()` (sensors API `POST events/{gundi_id}/attachments`). Per-event Redis state grows a `seen_file_ids` list (mirroring `seen_note_ids`) so files are forwarded exactly once, including files added to an event after it was first synced. The behavior is gated behind a new `include_attachments` config flag, default off, so existing connections are unaffected until an operator opts in.

**Tech Stack:** Python 3.10, FastAPI action-runner template, `earthranger-client` (erclient ≥1.15.0, has `AsyncERClient.get_file`), `gundi-client-v2` sensors API, pytest + pytest-asyncio + pytest-mock.

**Companion plan:** the destination side (CMORE consuming `Attachment` payloads) is `docs/superpowers/plans/2026-07-30-cmore-attachment-delivery.md` in the `gundi-integration-cmore` repo. This plan is independently testable and already useful for ER→ER connections (the classic ER dispatcher handles attachments today).

## Global Constraints

- Pydantic config models with `FieldWithUIOptions` for portal-rendered fields (existing pattern in `app/actions/configurations.py`).
- Per-event state lives in Redis via `state_manager` keyed by `(integration_id, "pull_events", er_event_uuid)`; the caller persists state AFTER forwarding succeeds so a crash mid-run re-detects on the next run (existing pattern — see `_emit_event_updates` docstring).
- Tests mock `AsyncERClient` via `mocker.patch("app.actions.handlers.AsyncERClient", ...)` and Gundi senders via `mocker.patch("app.actions.handlers.send_..._to_gundi", ...)` (existing patterns in `app/actions/tests/test_actions.py` + `conftest.py`).
- Run tests with `pytest`.

## Key facts an implementer needs (verified during research)

- **ER file entry shape** (from `das` `FileSerializerMixin.to_representation` + `EventFileSerializer`): `{"id": <uuid>, "url": "<host>/api/v1.0/activity/event/<event_id>/file/<file_id>/", "filename": "photo.jpg", "file_type": "image"|"file", "comment": "", "created_at": ..., "updates": [...], "images": {<size>: <url>}?, "icon_url": ...}`. `include_files` defaults to `true` on the events-list endpoint, and `erclient.get_events(**kwargs)` passes kwargs through as query params — so `er_event["files"]` is already present in what `pull_events` fetches today.
- **Download:** `AsyncERClient.get_file(url)` (erclient 1.15.0) GETs the URL with auth headers and returns the httpx response; bytes are `response.content`. Use the entry's `url` field (the original file), not the `images` renditions.
- **Upload to Gundi:** `app/services/gundi.py::send_event_attachments_to_gundi(event_id=<gundi_object_id>, attachments=[(filename, file_bytes)], integration_id=...)` already exists (wraps `GundiDataSenderClient.post_event_attachments`, with stamina retry on `httpx.HTTPError`).
- **Routing downstream:** the sensors API stores the file in Gundi's attachments bucket and publishes `AttachmentReceived`; `cdip-routing` routes it like any observation — classic destinations get a dispatcher delivery, generic-model destinations (CMORE) get a `GundiDelivery` whose payload is `Attachment` with `related_to` = the event's gundi_id. Nothing to change in routing.
- **First-sight semantics differ from notes deliberately:** existing notes are marked seen-without-forwarding on first sight (they're conversation history). Files ARE forwarded on first sight — the photo is part of the event itself, and the whole point is delivering it.
- **`pull_events` structure today** (`app/actions/handlers.py::action_pull_events`, ~line 381): builds `er_client = AsyncERClient(...)` (~line 404), then `async with er_client as earth_ranger:`; the event loop is `async for event_batch in earth_ranger.get_events(filter=..., batch_size=BATCH_SIZE, include_notes=True)`. New-event path posts via `send_events_to_gundi`, extracts `gundi_object_id`, then calls `_save_event_state(...)`. Seen-event path calls `_emit_event_updates(...)` then `_save_event_state(...)`.
- **`_save_event_state` signature today** (~line 1391): `(integration_id, er_event_uuid, gundi_object_id, er_event, seen_note_ids)` — persists `gundi_object_id`, `updated_at`, `state`, `priority`, `title`, `seen_note_ids`.

## File Structure

| File | Change | Responsibility |
|---|---|---|
| `app/actions/configurations.py` | Modify | `include_attachments` flag on `PullEventsConfig` |
| `app/actions/handlers.py` | Modify | `_forward_event_files()` helper; wire into new-event and updated-event paths; extend `_save_event_state` |
| `app/actions/tests/test_actions.py` | Modify | Tests for forwarding on first sight, dedupe via `seen_file_ids`, late-added files, flag-off behavior, per-file failure isolation |
| `docs/actions/` (pull_events doc page) | Modify | Document the new flag |

---

### Task 1: `include_attachments` config flag

**Files:**
- Modify: `app/actions/configurations.py` (class `PullEventsConfig`, ~line 202 — add after `force_run_since_start`)
- Test: `app/actions/tests/test_actions.py`

**Interfaces:**
- Produces: `PullEventsConfig.include_attachments: bool` (default `False`) — read by Task 3 in `action_pull_events`.

- [ ] **Step 1: Write the failing test**

Add to `app/actions/tests/test_actions.py`:

```python
def test_pull_events_config_attachments_flag_defaults_off():
    from app.actions.configurations import PullEventsConfig

    config = PullEventsConfig(start_datetime="2026-01-01T00:00:00Z")
    assert config.include_attachments is False

    config = PullEventsConfig(
        start_datetime="2026-01-01T00:00:00Z", include_attachments=True
    )
    assert config.include_attachments is True
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest app/actions/tests/test_actions.py::test_pull_events_config_attachments_flag_defaults_off -v`
Expected: FAIL — `include_attachments` is not a field, Pydantic ignores-or-errors depending on model config; the default-check assertion fails with `AttributeError`.

- [ ] **Step 3: Implement**

In `app/actions/configurations.py`, inside `PullEventsConfig`, add (after `force_run_since_start`, using the same `FieldWithUIOptions` style as the surrounding fields):

```python
    include_attachments: bool = FieldWithUIOptions(
        False,
        title="Forward Event Attachments",
        description=(
            "When enabled, files attached to ER events (photos, documents) are "
            "forwarded to Gundi as event attachments, so destinations that "
            "support them (e.g. EarthRanger, CMORE) receive the files. Files "
            "added to an event after it was first forwarded are picked up on "
            "subsequent runs. Off by default: enabling changes what existing "
            "downstream destinations receive."
        ),
    )
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest app/actions/tests/test_actions.py::test_pull_events_config_attachments_flag_defaults_off -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add app/actions/configurations.py app/actions/tests/test_actions.py
git commit -m "feat: include_attachments flag on PullEventsConfig (default off)"
```

---

### Task 2: `_forward_event_files()` helper

**Files:**
- Modify: `app/actions/handlers.py` (add near `_emit_event_updates`, ~line 1408)
- Test: `app/actions/tests/test_actions.py`

**Interfaces:**
- Consumes: `AsyncERClient.get_file(url) -> httpx.Response` (erclient); `send_event_attachments_to_gundi(event_id, attachments, integration_id=...)` — add it to the existing `from ..services.gundi import ...` line in `handlers.py`.
- Produces: `async def _forward_event_files(er_client, er_event, gundi_object_id, integration_id, seen_file_ids) -> tuple[int, list]` returning `(forwarded_count, updated_seen_file_ids)` — used by Task 3. Module constant `MAX_ATTACHMENT_BYTES = 20 * 1024 * 1024`.

- [ ] **Step 1: Write the failing tests**

Add to `app/actions/tests/test_actions.py`:

```python
def _file_entry(file_id="f-1", filename="photo.jpg"):
    return {
        "id": file_id,
        "filename": filename,
        "url": f"https://er.test/api/v1.0/activity/event/e-1/file/{file_id}/",
        "file_type": "image",
    }


def _mock_er_client_for_files(mocker, content=b"jpegbytes"):
    er_client = mocker.MagicMock()
    response = mocker.MagicMock()
    response.content = content
    er_client.get_file = mocker.AsyncMock(return_value=response)
    return er_client


@pytest.mark.asyncio
async def test_forward_event_files_downloads_and_posts_new_files(mocker):
    from app.actions.handlers import _forward_event_files

    er_client = _mock_er_client_for_files(mocker)
    send_mock = mocker.patch(
        "app.actions.handlers.send_event_attachments_to_gundi",
        mocker.AsyncMock(return_value={"object_id": "att-1"}),
    )

    er_event = {"id": "e-1", "files": [_file_entry("f-1"), _file_entry("f-2", "map.pdf")]}
    forwarded, seen = await _forward_event_files(
        er_client, er_event, "gundi-obj-1", "int-1", seen_file_ids=[]
    )

    assert forwarded == 2
    assert seen == ["f-1", "f-2"]
    assert send_mock.await_count == 2
    first_call = send_mock.await_args_list[0]
    assert first_call.kwargs["event_id"] == "gundi-obj-1"
    assert first_call.kwargs["attachments"] == [("photo.jpg", b"jpegbytes")]
    assert first_call.kwargs["integration_id"] == "int-1"


@pytest.mark.asyncio
async def test_forward_event_files_skips_already_seen(mocker):
    from app.actions.handlers import _forward_event_files

    er_client = _mock_er_client_for_files(mocker)
    send_mock = mocker.patch(
        "app.actions.handlers.send_event_attachments_to_gundi", mocker.AsyncMock()
    )

    er_event = {"id": "e-1", "files": [_file_entry("f-1"), _file_entry("f-2")]}
    forwarded, seen = await _forward_event_files(
        er_client, er_event, "gundi-obj-1", "int-1", seen_file_ids=["f-1"]
    )

    assert forwarded == 1
    assert seen == ["f-1", "f-2"]
    send_mock.assert_awaited_once()


@pytest.mark.asyncio
async def test_forward_event_files_failure_leaves_file_unseen_for_retry(mocker):
    from app.actions.handlers import _forward_event_files

    er_client = _mock_er_client_for_files(mocker)
    er_client.get_file = mocker.AsyncMock(side_effect=Exception("boom"))
    send_mock = mocker.patch(
        "app.actions.handlers.send_event_attachments_to_gundi", mocker.AsyncMock()
    )

    er_event = {"id": "e-1", "files": [_file_entry("f-1")]}
    forwarded, seen = await _forward_event_files(
        er_client, er_event, "gundi-obj-1", "int-1", seen_file_ids=[]
    )

    # Failed file is NOT marked seen → retried on the next pull.
    assert forwarded == 0
    assert seen == []
    send_mock.assert_not_awaited()


@pytest.mark.asyncio
async def test_forward_event_files_oversized_file_marked_seen_and_skipped(mocker):
    from app.actions.handlers import _forward_event_files, MAX_ATTACHMENT_BYTES

    er_client = _mock_er_client_for_files(mocker, content=b"x" * (MAX_ATTACHMENT_BYTES + 1))
    send_mock = mocker.patch(
        "app.actions.handlers.send_event_attachments_to_gundi", mocker.AsyncMock()
    )

    er_event = {"id": "e-1", "files": [_file_entry("f-big", "huge.mp4")]}
    forwarded, seen = await _forward_event_files(
        er_client, er_event, "gundi-obj-1", "int-1", seen_file_ids=[]
    )

    # Oversized: skipped, but marked seen so it isn't re-downloaded forever.
    assert forwarded == 0
    assert seen == ["f-big"]
    send_mock.assert_not_awaited()
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest app/actions/tests/test_actions.py -k forward_event_files -v`
Expected: FAIL with `ImportError: cannot import name '_forward_event_files'`.

- [ ] **Step 3: Implement**

In `app/actions/handlers.py`:

Extend the gundi-services import line:

```python
from ..services.gundi import send_events_to_gundi, send_observations_to_gundi, \
    update_event_in_gundi, send_event_attachments_to_gundi
```

Add near `_emit_event_updates`:

```python
# Guardrail: don't pull pathological uploads (videos, raw camera dumps) into
# runner memory / the attachments bucket. Oversized files are marked seen so
# they aren't re-downloaded on every run.
MAX_ATTACHMENT_BYTES = 20 * 1024 * 1024


async def _forward_event_files(er_client, er_event, gundi_object_id, integration_id, seen_file_ids):
    """Forward not-yet-seen ER event files to Gundi as event attachments.

    Unlike notes (marked seen-without-forwarding on first sight), files ARE
    forwarded the first time an event is seen — the attachment is part of the
    event itself, not conversation history.

    Returns (forwarded_count, updated_seen_file_ids). A per-file failure is
    logged and the file is left out of seen_file_ids so the next run retries
    it; one bad file never blocks the rest of the event (or the run).
    """
    seen = list(seen_file_ids)
    seen_set = set(seen)
    forwarded = 0
    for entry in er_event.get("files") or []:
        file_id = entry.get("id")
        url = entry.get("url")
        if not file_id or file_id in seen_set or not url:
            continue
        filename = entry.get("filename") or f"attachment-{file_id}"
        try:
            response = await er_client.get_file(url)
            content = response.content
            if len(content) > MAX_ATTACHMENT_BYTES:
                logger.warning(
                    "Skipping oversized ER file %s (%d bytes) on event %s.",
                    file_id, len(content), er_event.get("id"),
                )
                seen.append(file_id)
                seen_set.add(file_id)
                continue
            await send_event_attachments_to_gundi(
                event_id=gundi_object_id,
                attachments=[(filename, content)],
                integration_id=integration_id,
            )
        except Exception:
            logger.exception(
                "Failed to forward ER file %s on event %s; will retry next run.",
                file_id, er_event.get("id"),
            )
            continue
        seen.append(file_id)
        seen_set.add(file_id)
        forwarded += 1
    return forwarded, seen
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest app/actions/tests/test_actions.py -k forward_event_files -v`
Expected: PASS (4 tests).

- [ ] **Step 5: Commit**

```bash
git add app/actions/handlers.py app/actions/tests/test_actions.py
git commit -m "feat: _forward_event_files helper for ER event attachments"
```

---

### Task 3: Wire attachments into `pull_events` + state

**Files:**
- Modify: `app/actions/handlers.py` — `_save_event_state` (~line 1391), the new-event and updated-event paths inside `action_pull_events` (~lines 525–590), and the summary return dict / log line at the end of the action.
- Test: `app/actions/tests/test_actions.py`

**Interfaces:**
- Consumes: `_forward_event_files(er_client, er_event, gundi_object_id, integration_id, seen_file_ids)` from Task 2 (called with `earth_ranger`, the entered client, as `er_client`); `pull_config.include_attachments` from Task 1.
- Produces: `_save_event_state(integration_id, er_event_uuid, gundi_object_id, er_event, seen_note_ids, seen_file_ids=None)` — one new keyword arg, defaulting to `None` (stored as `[]`), so the two existing call sites keep working while being updated. Run summary gains `"attachments_forwarded": <int>`.

- [ ] **Step 1: Write the failing tests**

Add to `app/actions/tests/test_actions.py`. These follow the repo's existing integration-test style for `pull_events` — reuse the fixtures the neighbouring tests use (`mock_gundi_client_v2_class`, `mock_state_manager`, `er_integration_v2`, `mock_publish_event`, etc.; copy the fixture list from the nearest existing `test_execute_pull_events_*` test) and extend the erclient mock with `get_file`:

```python
@pytest.mark.asyncio
async def test_pull_events_forwards_files_when_flag_enabled(
    mocker,
    mock_gundi_client_v2_class,
    mock_erclient_class,
    mock_state_manager,
    mock_publish_event,
    er_integration_v2,
    mock_gundi_sensors_client_class,
    mock_get_gundi_api_key,
):
    from app.actions.handlers import action_pull_events
    from app.actions.configurations import PullEventsConfig

    mocker.patch("app.actions.handlers.AsyncERClient", mock_erclient_class)
    mocker.patch("app.actions.handlers.state_manager", mock_state_manager)
    # Never-seen event → new-event path.
    mock_state_manager.get_state.return_value = {}

    # One event carrying one file.
    file_response = mocker.MagicMock()
    file_response.content = b"jpegbytes"
    erclient_instance = mock_erclient_class.return_value.__aenter__.return_value
    erclient_instance.get_file = mocker.AsyncMock(return_value=file_response)
    er_event = {
        "id": "er-uuid-1",
        "updated_at": "2026-07-30T00:00:00Z",
        "title": "Rhino carcass",
        "files": [{
            "id": "f-1",
            "filename": "photo.jpg",
            "url": "https://er.test/api/v1.0/activity/event/er-uuid-1/file/f-1/",
            "file_type": "image",
        }],
    }
    erclient_instance.get_events.return_value = AsyncIterator([[er_event]])

    send_events_mock = mocker.patch(
        "app.actions.handlers.send_events_to_gundi",
        mocker.AsyncMock(return_value=[{"object_id": "gundi-obj-1"}]),
    )
    send_attachments_mock = mocker.patch(
        "app.actions.handlers.send_event_attachments_to_gundi",
        mocker.AsyncMock(return_value={"object_id": "att-1"}),
    )

    config = PullEventsConfig(
        start_datetime="2026-01-01T00:00:00Z", include_attachments=True
    )
    result = await action_pull_events(er_integration_v2, config)

    send_events_mock.assert_awaited_once()
    send_attachments_mock.assert_awaited_once()
    assert send_attachments_mock.await_args.kwargs["event_id"] == "gundi-obj-1"
    assert send_attachments_mock.await_args.kwargs["attachments"] == [("photo.jpg", b"jpegbytes")]
    assert result["attachments_forwarded"] == 1
    # seen_file_ids persisted so the file isn't re-sent next run.
    saved_states = [c.kwargs for c in mock_state_manager.set_state.await_args_list]
    per_event_state = next(s for s in saved_states if s.get("source_id") == "er-uuid-1")
    assert per_event_state["state"]["seen_file_ids"] == ["f-1"]


@pytest.mark.asyncio
async def test_pull_events_ignores_files_when_flag_disabled(
    mocker,
    mock_gundi_client_v2_class,
    mock_erclient_class,
    mock_state_manager,
    mock_publish_event,
    er_integration_v2,
    mock_gundi_sensors_client_class,
    mock_get_gundi_api_key,
):
    from app.actions.handlers import action_pull_events
    from app.actions.configurations import PullEventsConfig

    mocker.patch("app.actions.handlers.AsyncERClient", mock_erclient_class)
    mocker.patch("app.actions.handlers.state_manager", mock_state_manager)
    mock_state_manager.get_state.return_value = {}

    erclient_instance = mock_erclient_class.return_value.__aenter__.return_value
    erclient_instance.get_file = mocker.AsyncMock()
    er_event = {
        "id": "er-uuid-1",
        "updated_at": "2026-07-30T00:00:00Z",
        "title": "Rhino carcass",
        "files": [{
            "id": "f-1",
            "filename": "photo.jpg",
            "url": "https://er.test/api/v1.0/activity/event/er-uuid-1/file/f-1/",
            "file_type": "image",
        }],
    }
    erclient_instance.get_events.return_value = AsyncIterator([[er_event]])

    mocker.patch(
        "app.actions.handlers.send_events_to_gundi",
        mocker.AsyncMock(return_value=[{"object_id": "gundi-obj-1"}]),
    )
    send_attachments_mock = mocker.patch(
        "app.actions.handlers.send_event_attachments_to_gundi", mocker.AsyncMock()
    )

    config = PullEventsConfig(start_datetime="2026-01-01T00:00:00Z")  # flag off
    result = await action_pull_events(er_integration_v2, config)

    send_attachments_mock.assert_not_awaited()
    erclient_instance.get_file.assert_not_awaited()
    assert result["attachments_forwarded"] == 0


@pytest.mark.asyncio
async def test_pull_events_forwards_late_added_file_on_seen_event(
    mocker,
    mock_gundi_client_v2_class,
    mock_erclient_class,
    mock_state_manager,
    mock_publish_event,
    er_integration_v2,
    mock_gundi_sensors_client_class,
    mock_get_gundi_api_key,
):
    from app.actions.handlers import action_pull_events
    from app.actions.configurations import PullEventsConfig

    mocker.patch("app.actions.handlers.AsyncERClient", mock_erclient_class)
    mocker.patch("app.actions.handlers.state_manager", mock_state_manager)
    # Previously-seen event: f-1 already forwarded; ER shows a new f-2.
    mock_state_manager.get_state.return_value = {
        "gundi_object_id": "gundi-obj-1",
        "updated_at": "2026-07-29T00:00:00Z",
        "title": "Rhino carcass",
        "seen_note_ids": [],
        "seen_file_ids": ["f-1"],
    }

    file_response = mocker.MagicMock()
    file_response.content = b"newbytes"
    erclient_instance = mock_erclient_class.return_value.__aenter__.return_value
    erclient_instance.get_file = mocker.AsyncMock(return_value=file_response)
    er_event = {
        "id": "er-uuid-1",
        "updated_at": "2026-07-30T00:00:00Z",  # advanced → update path runs
        "title": "Rhino carcass",
        "files": [
            {"id": "f-1", "filename": "photo.jpg",
             "url": "https://er.test/api/v1.0/activity/event/er-uuid-1/file/f-1/",
             "file_type": "image"},
            {"id": "f-2", "filename": "second.jpg",
             "url": "https://er.test/api/v1.0/activity/event/er-uuid-1/file/f-2/",
             "file_type": "image"},
        ],
    }
    erclient_instance.get_events.return_value = AsyncIterator([[er_event]])

    send_attachments_mock = mocker.patch(
        "app.actions.handlers.send_event_attachments_to_gundi",
        mocker.AsyncMock(return_value={"object_id": "att-2"}),
    )
    mocker.patch("app.actions.handlers.update_event_in_gundi", mocker.AsyncMock())

    config = PullEventsConfig(
        start_datetime="2026-01-01T00:00:00Z", include_attachments=True
    )
    result = await action_pull_events(er_integration_v2, config)

    send_attachments_mock.assert_awaited_once()
    assert send_attachments_mock.await_args.kwargs["attachments"] == [("second.jpg", b"newbytes")]
    assert result["attachments_forwarded"] == 1
    saved_states = [c.kwargs for c in mock_state_manager.set_state.await_args_list]
    per_event_state = next(s for s in saved_states if s.get("source_id") == "er-uuid-1")
    assert per_event_state["state"]["seen_file_ids"] == ["f-1", "f-2"]
```

Note: if a fixture named above doesn't exist under that exact name in this repo's `conftest.py`, use the name the neighbouring `test_execute_pull_events_*` tests use — the shapes are what matters (a mocked `AsyncERClient` class whose entered instance serves `get_events`/`get_file`, a mocked `state_manager`, and mocked Gundi senders).

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest app/actions/tests/test_actions.py -k "pull_events and (forwards_files or ignores_files or late_added)" -v`
Expected: FAIL — `result` has no `attachments_forwarded` key and `send_event_attachments_to_gundi` is never awaited.

- [ ] **Step 3: Implement**

In `app/actions/handlers.py`:

**3a.** Extend `_save_event_state`:

```python
async def _save_event_state(integration_id, er_event_uuid, gundi_object_id, er_event,
                            seen_note_ids, seen_file_ids=None):
    """Persist per-event state under (pull_events, er_event_uuid)."""
    await state_manager.set_state(
        integration_id=integration_id,
        action_id="pull_events",
        source_id=er_event_uuid,
        state={
            "gundi_object_id": gundi_object_id,
            "updated_at": er_event.get("updated_at"),
            "state": er_event.get("state"),
            "priority": er_event.get("priority"),
            "title": er_event.get("title"),
            "seen_note_ids": list(seen_note_ids),
            "seen_file_ids": list(seen_file_ids or []),
        },
    )
```

**3b.** In `action_pull_events`, add a counter next to the existing ones (`events_new = 0` etc.):

```python
    attachments_forwarded = 0
```

**3c.** New-event path — after `gundi_object_id` is extracted and before the existing `_save_event_state(...)` call, forward files, then pass `seen_file_ids` through:

```python
                    # Forward files attached to the event (photos, documents)
                    # before persisting state: a crash between post and save
                    # re-runs this event next pull and the seen-list dedupes.
                    seen_file_ids = []
                    if pull_config.include_attachments:
                        forwarded, seen_file_ids = await _forward_event_files(
                            earth_ranger, er_event, gundi_object_id,
                            integration_id, [],
                        )
                        attachments_forwarded += forwarded
                    # Mark all existing notes as already-seen (no bulk-forward on first sight).
                    note_ids = [n["id"] for n in er_event.get("notes") or [] if n.get("id")]
                    await _save_event_state(
                        integration_id=integration_id,
                        er_event_uuid=er_event_uuid,
                        gundi_object_id=gundi_object_id,
                        er_event=er_event,
                        seen_note_ids=note_ids,
                        seen_file_ids=seen_file_ids,
                    )
```

(The `note_ids` line and `_save_event_state` call already exist — this inserts the file-forwarding block above them and adds the `seen_file_ids=` argument.)

**3d.** Updated-event path — after `_emit_event_updates(...)` returns and before the existing `_save_event_state(...)` call:

```python
                seen_file_ids = state_record.get("seen_file_ids", [])
                if pull_config.include_attachments:
                    forwarded, seen_file_ids = await _forward_event_files(
                        earth_ranger, er_event, state_record["gundi_object_id"],
                        integration_id, seen_file_ids,
                    )
                    attachments_forwarded += forwarded
                # Refresh state to reflect what we forwarded this run.
                await _save_event_state(
                    integration_id=integration_id,
                    er_event_uuid=er_event_uuid,
                    gundi_object_id=state_record["gundi_object_id"],
                    er_event=er_event,
                    seen_note_ids=new_seen_note_ids,
                    seen_file_ids=seen_file_ids,
                )
```

**3e.** Extend the final summary log and return dict of `action_pull_events`:

```python
    logger.info(
        f"pull_events done. new={events_new} updated={events_updated} "
        f"updates_emitted={updates_emitted} skipped_unchanged={events_skipped_unchanged} "
        f"attachments_forwarded={attachments_forwarded}"
    )
    return {
        "events_extracted": events_new,
        "events_updated": events_updated,
        "updates_emitted": updates_emitted,
        "events_skipped_unchanged": events_skipped_unchanged,
        "attachments_forwarded": attachments_forwarded,
    }
```

(Match the actual key names of the existing return dict — extend it, don't rename existing keys. The early-return dicts for unresolvable filters should also gain `"attachments_forwarded": 0` for a consistent shape.)

- [ ] **Step 4: Run the tests and the full suite**

Run: `pytest app/actions/tests/test_actions.py -v && pytest`
Expected: the 3 new tests PASS; all pre-existing `pull_events` tests still PASS (flag defaults off, `_save_event_state` new arg is optional).

- [ ] **Step 5: Update docs**

In the pull_events page under `docs/actions/` (and `docs/configuration.md` if it lists config fields), add:

```markdown
### Forward Event Attachments (`include_attachments`)

Off by default. When enabled, files attached to ER events (photos, documents)
are forwarded to Gundi as event attachments and delivered to destinations that
support them (EarthRanger, CMORE). Each file is forwarded once (tracked
per-event in Redis as `seen_file_ids`); files added to an event later are
picked up on the next pull. Files over 20 MB are skipped with a warning.
```

- [ ] **Step 6: Commit**

```bash
git add app/actions/handlers.py app/actions/tests/test_actions.py docs/
git commit -m "feat: forward ER event files to Gundi as attachments (opt-in)"
```

---

### Task 4: Rollout checklist (manual, no code)

- [ ] Deploy the runner; no new env vars or permissions are needed on this side (uploads go through the sensors API with the integration's existing API key).
- [ ] Enable **Forward Event Attachments** on the ER-provider integration of the ER↔CMORE connection in the Gundi portal (pull_events config).
- [ ] Verify in Gundi's activity logs that `AttachmentReceived` events appear after the next pull of an event that has a photo.
- [ ] End-to-end (requires the companion CMORE plan deployed): confirm the photo lands as a media comment on the CMORE event.
- [ ] Watch the first enabled runs for volume: a backfill window with photo-heavy events will download every file once. If needed, enable the flag only after the initial backfill completes.

---

## Self-Review

- **Spec coverage:** reading files from ER (Task 2: `get_file` on the entry `url`), sending to Gundi (Task 2: `send_event_attachments_to_gundi`), exactly-once + late-file handling (Task 3 state), opt-in gating (Task 1), rollout (Task 4).
- **Placeholder scan:** all steps carry concrete code; the only soft spot is fixture names in Task 3's tests, which is called out explicitly with instructions to mirror the neighbouring tests rather than guess.
- **Type consistency:** `_forward_event_files(er_client, er_event, gundi_object_id, integration_id, seen_file_ids) -> (int, list)` identical between Task 2 implementation/tests and Task 3 call sites; `_save_event_state(..., seen_file_ids=None)` matches both updated call sites; `include_attachments` name matches Task 1 and Task 3.
