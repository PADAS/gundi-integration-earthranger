"""The activity log's detail lines: what each Share run did at a destination, or left out and why."""
from unittest.mock import AsyncMock

import pytest
from gundi_core.events import LogLevel

from app.actions.configurations import SyncEventsConfig, SyncObservationsConfig
from app.ersharing import reporting
from app.ersharing.reporting import DestinationReport, names, publish_report, say
from app.ersharing.state import EVENTS_ACTION
from app.ersharing.sync_events import run_sync_events
from app.ersharing.sync_observations import run_sync_observations
from app.ersharing.tests import test_sync_events_run as events_run
from app.ersharing.tests import test_sync_observations as observations_run
from app.ersharing.tests.fakes import make_event

# The runs' fixtures, reused under their own names.
events_world = events_run.world
tracks_world = observations_run.world


def titles(report: DestinationReport):
    return [g.title(g.count, g.items) for g in report.details.values()]


def test_names_lists_five_and_counts_the_rest():
    items = {f"s{i}": i for i in range(1, 8)}
    assert names(items, 7) == "s1, s2, s3, s4, s5 and 2 more"
    assert names({"a": 3, "b": 1}, 4, with_counts=True) == "a 3, b 1"


def test_say_uses_the_single_form_for_one_item():
    title = say("Event {name} deleted", "{count} events deleted: {names}")
    assert title(1, {"#7": 1}) == "Event #7 deleted"
    assert title(2, {"#7": 1, "#8": 1}) == "2 events deleted: #7, #8"


@pytest.mark.asyncio
async def test_details_are_published_as_info_lines_after_the_summary(monkeypatch):
    log = AsyncMock()
    monkeypatch.setattr(reporting, "log_action_activity", log)
    report = DestinationReport(destination="Site B", kind="Events", created=2)
    report.detail("deleted", say("Event {name} deleted", "{count} events deleted: {names}"), item="#7")
    await publish_report("i1", EVENTS_ACTION, report)
    detail = log.await_args_list[1].kwargs
    assert detail["title"] == "ER2ER Event Sharing → Site B: Event #7 deleted"
    assert detail["level"] == LogLevel.INFO
    assert detail["data"] == {"count": 1, "items": {"#7": 1}}
    assert report.as_dict()["details"] == {"deleted": {"count": 1, "items": {"#7": 1}}}


@pytest.mark.asyncio
async def test_events_run_logs_its_setup_then_what_it_shared(events_world):
    await run_sync_events(events_run.INTEGRATION, events_run.CONFIG)
    assert titles(events_world.published[0]) == [
        "First run: sharing events updated since 2026-09-01 00:00 UTC",
        "Event category Sitea-c created",
        "Event type Sitea-W created in Sitea-c",
        "1 event shared: Sitea-W",
    ]


@pytest.mark.asyncio
async def test_events_left_out_by_the_type_filter_are_logged(events_world):
    events_world.source.events["e2"] = make_event("e2", "2026-10-02T00:00:00Z", event_type="other_type")
    config = SyncEventsConfig.parse_obj({"start_from": "2026-09-01T00:00:00Z", "event_types": ["wildlife_sighting"]})
    await run_sync_events(events_run.INTEGRATION, config)
    assert ("1 event not shared because its event type isn't one of the shared event types: other_type"
            in titles(events_world.published[0]))


@pytest.mark.asyncio
async def test_default_first_run_says_how_far_back_it_starts(events_world):
    await run_sync_events(events_run.INTEGRATION, SyncEventsConfig.parse_obj({}))
    assert titles(events_world.published[0])[0].endswith("UTC (the last 10 days)")


@pytest.mark.asyncio
async def test_tracks_run_logs_subjects_points_and_what_it_left_out(tracks_world):
    config = SyncObservationsConfig.parse_obj({"start_from": "2026-09-30T00:00:00Z", "subject_groups": ["g1"]})
    await run_sync_observations(observations_run.INTEGRATION, config)
    lines = titles(tracks_world.published[0])
    assert "First share for subject Lion 1: tracks from 2026-09-30 18:00 UTC (the last 12 hours)" in lines
    assert tracks_world.published[0].summary_title().startswith(
        "ER2ER Observation Sharing → Site 1: 2 points shared from 1 subject to subject group Sitea, 0 failed")
    assert not any(t.startswith("2 points shared") for t in lines)  # one subject: the subjects line names it
    assert "Subject Lion 1 from subject group Lions shared to subject group Sitea" in lines
    assert "1 point not shared because it has no valid location: Lion 1" in lines


@pytest.mark.asyncio
async def test_subject_without_a_device_is_logged(tracks_world):
    tracks_world.source.assignments = [a for a in tracks_world.source.assignments if a["subject"] != "s2"]
    await run_sync_observations(observations_run.INTEGRATION, observations_run.CONFIG)
    assert "Subject Zebra 1 not shared because it has no device assigned" in titles(tracks_world.published[0])


@pytest.mark.asyncio
async def test_subject_with_nothing_new_is_logged_on_the_next_run(tracks_world):
    await run_sync_observations(observations_run.INTEGRATION, observations_run.CONFIG)
    tracks_world.published.clear()
    await run_sync_observations(observations_run.INTEGRATION, observations_run.CONFIG)
    assert any(t.startswith("2 subjects had no new points to share") for t in titles(tracks_world.published[0]))


@pytest.mark.asyncio
async def test_points_per_subject_are_listed_when_they_differ(tracks_world):
    await run_sync_observations(observations_run.INTEGRATION, observations_run.CONFIG)
    lines = titles(tracks_world.published[0])
    points = [t for t in lines if "points shared:" in t]
    subjects = [t for t in lines if "subjects from" in t]
    assert len(points) + len(subjects) == 1


def test_a_quiet_run_says_nothing_new():
    report = DestinationReport(destination="Site B", kind="Events")
    assert report.is_quiet()
    assert report.summary_title() == "ER2ER Event Sharing → Site B: nothing new to share"


@pytest.mark.asyncio
async def test_a_quiet_summary_is_logged_at_most_once_a_day(monkeypatch):
    from app.ersharing.state import ShareState
    from app.ersharing.tests.fakes import InMemoryStateManager
    log = AsyncMock()
    monkeypatch.setattr(reporting, "log_action_activity", log)
    state = ShareState("i1", manager=InMemoryStateManager())
    for _ in range(3):
        await publish_report("i1", EVENTS_ACTION, DestinationReport("Site B", "Events"), state=state,
                             destination_id="d1")
    assert log.await_count == 1


@pytest.mark.asyncio
async def test_an_overlapping_run_says_why_it_stopped(events_world):
    from app.ersharing.state import ShareState
    await ShareState("i1", manager=events_world.manager).try_lease(EVENTS_ACTION, 60)
    await run_sync_events(events_run.INTEGRATION, events_run.CONFIG)
    titles_logged = [c.kwargs["title"] for c in events_world.logs.await_args_list]
    assert titles_logged == ["ER2ER Event Sharing didn't run this time because its previous run was still going."]



@pytest.mark.asyncio
async def test_subjects_line_appears_only_when_a_subject_joins():
    from app.ersharing.state import ShareState
    from app.ersharing.sync_observations import _drop_known_subjects
    from app.ersharing.tests.fakes import InMemoryStateManager
    state = ShareState("i1", manager=InMemoryStateManager())
    title = say("Subject {name} shared", "{count} subjects shared: {names}")

    async def subjects_line(*subjects):
        report = DestinationReport(destination="Site 1", kind="Tracks")
        for name in subjects:
            report.detail("subjects", title, item=name)
        await _drop_known_subjects(state, "d1", report)
        return "subjects" in report.details

    assert await subjects_line("Lion 1", "Zebra 1")
    assert not await subjects_line("Lion 1", "Zebra 1")
    assert not await subjects_line("Lion 1")
    assert await subjects_line("Lion 1", "Rhino 1")
    assert not await subjects_line("Zebra 1", "Rhino 1")
