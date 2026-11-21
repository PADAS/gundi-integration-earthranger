"""Fixes from a local review round of the merged runner."""
import asyncio
from datetime import timedelta
from unittest.mock import AsyncMock

import pytest

from app.actions.configurations import SyncEventsConfig, SyncObservationsConfig
from app.ersharing import preview, sync_events, sync_observations
from app.ersharing.event_types import EventTypeSync
from app.ersharing.reporting import DestinationReport
from app.ersharing.state import EVENTS_ACTION, Marker
from app.ersharing.sync_events import marker_key, reconcile_destination, run_sync_events
from app.ersharing.sync_observations import run_sync_observations
from app.ersharing.tests.fakes import FakeSite, make_event
from app.ersharing.tests.test_sync_events import DESTINATION, setup, sync
from app.ersharing.tests.test_sync_events_run import (
    CONFIG, INTEGRATION, destination, provenance, reconcile_run, world)  # noqa: F401 (fixture)
from app.ersharing.timeutil import parse_dt, utcnow
from app.services.errors import IntegrationAuthError


@pytest.mark.asyncio
async def test_an_unfinished_copy_of_an_event_no_longer_shared_comes_off_the_list():
    config = SyncEventsConfig.parse_obj({"start_from": "2026-09-01T00:00:00Z", "event_types": ["wildlife_sighting"]})
    source, dest, state, run = setup(make_event("e1", "2026-10-01T00:00:00Z", event_type="fire"), config=config)
    await state.set_marker(EVENTS_ACTION, marker_key("d1"), Marker(since=parse_dt("2026-10-02T00:00:00Z"),
                                                                     advanced_at=utcnow()))
    await state.set_pending_events("d1", ["e1"])

    await sync(run, dest)

    assert await state.get_pending_events("d1") == []
    source.calls.clear()
    _, _, _, rerun = setup(config=config)
    rerun.source, rerun.state = source, state
    await sync(rerun, dest)
    assert not source.calls_to("get_event")


@pytest.mark.asyncio
async def test_a_catch_up_cut_short_by_time_resumes_after_the_events_it_handled():
    source, dest, state, run = setup()
    base = utcnow() - timedelta(days=3)
    times = [(base + timedelta(hours=n)).isoformat() for n in (1, 2, 3)]
    for n, at in enumerate(times, start=1):
        source.events[f"e{n}"] = make_event(f"e{n}", at)
    types = EventTypeSync(source, dest, run.settings)
    types.waiting = {"wildlife_sighting": base}
    run.out_of_time = lambda: "e2" in dest.events
    report = DestinationReport("Site B", "Events")

    await sync_events.catch_up_waiting_types(run, DESTINATION, dest, types, report)

    assert report.timed_out and set(dest.events) == {"e1", "e2"}
    assert types.waiting == {"wildlife_sighting": parse_dt(times[1])}


@pytest.mark.asyncio
async def test_share_events_finishes_when_the_follow_up_run_cant_be_started(world, monkeypatch):
    monkeypatch.setattr(sync_events, "BUDGET_FRACTION", -1)  # out of time immediately
    monkeypatch.setattr(sync_events, "trigger_action", AsyncMock(side_effect=RuntimeError("publish failed")))
    result = await run_sync_events(INTEGRATION, CONFIG)
    assert sync_events.trigger_action.await_count == 1
    assert len(result["destinations"]) == 2


@pytest.mark.asyncio
async def test_share_subjects_finishes_when_the_follow_up_run_cant_be_started(monkeypatch):
    from app.ersharing.tests import test_sync_observations as obs
    world_obs = obs.world.__wrapped__(monkeypatch)
    monkeypatch.setattr(sync_observations, "BUDGET_FRACTION", -1)
    monkeypatch.setattr(sync_observations, "trigger_action", AsyncMock(side_effect=RuntimeError("publish failed")))
    result = await run_sync_observations(obs.INTEGRATION, obs.CONFIG)
    assert sync_observations.trigger_action.await_count == 1
    assert all(r.timed_out for r in world_obs.published) and result is not None


@pytest.mark.asyncio
async def test_a_refused_preview_read_stops_the_other_reads():
    site = FakeSite("https://sitea.pamdas.org", role="source")
    stopped = []

    async def slow(*args, **kwargs):
        try:
            await asyncio.sleep(10)
        except asyncio.CancelledError:
            stopped.append(True)
            raise

    async def refused():
        raise IntegrationAuthError("Source site sitea.pamdas.org: forbidden", status_code=403)

    site.get_event_categories = refused
    site.list_event_types_v2 = slow
    site.list_event_types_v1 = slow

    result = await preview.preview_events(site, [])

    assert stopped == [True, True]
    assert result["total_count"] == 0 and result.get("warning")


@pytest.mark.asyncio
async def test_the_removal_check_counts_a_copy_listed_twice_once():
    source, dest = FakeSite("https://sitea.pamdas.org", role="source"), FakeSite()
    gone = make_event("gone", (utcnow() - timedelta(days=1)).isoformat(), event_details=provenance("gone"))
    dest.events["gone"] = gone

    async def listing(since, **kw):  # the listing repeats the copy at the time it restarts from
        yield [dict(gone)]
        yield [dict(gone)]

    dest.iter_events_updated_since = listing
    report = DestinationReport("Site B", "Events")

    assert await reconcile_destination(reconcile_run(source), destination(1), dest, report) is True
    assert dest.events == {} and report.deleted == 1
