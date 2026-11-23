"""Copied event types keep the source icon; subjects whose devices were deleted at the
source are removed from the destination once a day."""
import pytest

from app.actions.configurations import SyncObservationsConfig
from app.ersharing.event_types import DEFAULT_ICON, EventTypeSync
from app.ersharing.modes import SIMPLE_PROVIDER_KEY
from app.ersharing.state import OBSERVATIONS_ACTION
from app.ersharing.subject_cleanup import cleanup_key
from app.ersharing.sync_observations import run_sync_observations
from app.ersharing.tests import test_event_types as types_
from app.ersharing.tests import test_sync_observations as tracks
from app.services.errors import IntegrationAuthError

world = tracks.world
STAMP = "site:sitea.pamdas.org#user:svc#subject:x#rts:1"


def icon_sites(icon_id="wildlife_sighting_rep", dest_icons=None):
    source, dest = types_.sites()
    source.event_types["wildlife_sighting"]["icon_id"] = icon_id
    dest.icons = dest_icons
    return source, dest


@pytest.mark.asyncio
async def test_copied_type_keeps_the_source_icon_when_the_destination_has_it():
    source, dest = icon_sites(dest_icons={"wildlife_sighting_rep", "generic_rep"})
    await EventTypeSync(source, dest, types_.SETTINGS).ensure("wildlife_sighting")
    sent = dest.calls_to("post_event_type_v2")[0][0]
    assert sent["icon"] == "wildlife_sighting_rep" and "icon_id" not in sent


@pytest.mark.asyncio
async def test_copied_type_gets_the_default_icon_when_the_destination_lacks_it():
    source, dest = icon_sites(icon_id="custom_icon", dest_icons={"generic_rep"})
    await EventTypeSync(source, dest, types_.SETTINGS).ensure("wildlife_sighting")
    assert dest.calls_to("post_event_type_v2")[0][0]["icon"] == DEFAULT_ICON


@pytest.mark.asyncio
async def test_source_icon_is_kept_when_the_destination_cant_list_icons():
    source, dest = icon_sites(icon_id="custom_icon", dest_icons=None)
    await EventTypeSync(source, dest, types_.SETTINGS).ensure("wildlife_sighting")
    assert dest.calls_to("post_event_type_v2")[0][0]["icon"] == "custom_icon"


@pytest.mark.asyncio
async def test_a_type_with_the_wrong_icon_is_fixed_once():
    source, dest = icon_sites(dest_icons={"wildlife_sighting_rep"})
    await EventTypeSync(source, dest, types_.SETTINGS).ensure("wildlife_sighting")
    dest.event_types["sitea_wildlife_sighting"]["icon_id"] = "sitea_wildlife_sighting"
    await EventTypeSync(source, dest, types_.SETTINGS).ensure("wildlife_sighting")
    assert dest.calls_to("patch_event_type_v2")[0][0]["icon"] == "wildlife_sighting_rep"
    assert dest.event_types["sitea_wildlife_sighting"]["icon_id"] == "wildlife_sighting_rep"


def shared_subject(site, subject_id, name, device_id, provider=SIMPLE_PROVIDER_KEY, stamp=STAMP):
    site.subject_groups[0]["subjects"].append({"id": subject_id, "name": name})
    site.subject_sources[subject_id] = [{"id": f"dev-{subject_id}", "manufacturer_id": device_id,
                                         "provider": provider}]
    site.latest_observations[subject_id] = {"observation_details": {"sharing_lineage": [stamp]}}


def dest_with_group(world, group="Sitea"):
    dest = world.sites["https://site1.pamdas.org"]
    dest.subject_groups = [{"id": "dg", "name": group, "subjects": []}]
    for other in ("https://site2.pamdas.org",):
        world.sites[other].subject_groups = []
    return dest


@pytest.mark.asyncio
async def test_subject_whose_device_is_gone_is_removed(world):
    dest = dest_with_group(world)
    world.source.sources = {"src-s1": {"id": "src-s1"}}
    shared_subject(dest, "d-lion", "Lion 1", "src-s1")
    shared_subject(dest, "d-old", "Lion 1 (old)", "src-gone")
    await run_sync_observations(tracks.INTEGRATION, tracks.CONFIG)
    assert dest.deleted_subjects == ["d-old"]
    report = world.published[0]
    assert report.deleted == 1
    assert any(t == "Subject Lion 1 (old) removed because its device was deleted at the source"
               for t in [g.title(g.count, g.items) for g in report.details.values()])


@pytest.mark.asyncio
async def test_subjects_from_other_shares_are_left_alone(world):
    dest = dest_with_group(world)
    shared_subject(dest, "d-other-site", "Rhino", "gone-1", stamp="site:sitec.pamdas.org#user:u")
    shared_subject(dest, "d-other-key", "Elephant", "gone-2", provider="some_other_provider")
    dest.subject_groups[0]["subjects"].append({"id": "d-manual", "name": "Ranger"})
    await run_sync_observations(tracks.INTEGRATION, tracks.CONFIG)
    assert dest.deleted_subjects == []


@pytest.mark.asyncio
async def test_cleanup_runs_once_a_day(world):
    dest = dest_with_group(world)
    await run_sync_observations(tracks.INTEGRATION, tracks.CONFIG)
    shared_subject(dest, "d-old", "Old", "src-gone")
    await run_sync_observations(tracks.INTEGRATION, tracks.CONFIG)
    assert dest.deleted_subjects == []
    state_key = ("i1", OBSERVATIONS_ACTION, cleanup_key("d1"))
    assert state_key in world.manager.data


@pytest.mark.asyncio
async def test_cleanup_can_be_turned_off(world):
    dest = dest_with_group(world)
    shared_subject(dest, "d-old", "Old", "src-gone")
    config = SyncObservationsConfig.parse_obj({"start_from": "2026-09-30T00:00:00Z",
                                               "remove_deleted_subjects": False})
    await run_sync_observations(tracks.INTEGRATION, config)
    assert dest.deleted_subjects == []


@pytest.mark.asyncio
async def test_source_refusing_to_show_devices_is_a_warning_not_a_removal(world):
    dest = dest_with_group(world)
    shared_subject(dest, "d-old", "Old", "src-gone")
    world.source.failures["get_source"] = IntegrationAuthError("forbidden", status_code=403)
    await run_sync_observations(tracks.INTEGRATION, tracks.CONFIG)
    assert dest.deleted_subjects == []
    assert any("weren't checked for removal" in title for title in world.published[0].errors)


@pytest.mark.asyncio
async def test_icon_list_server_error_keeps_the_source_icon():
    from app.services.errors import IntegrationBadResponseError
    source, dest = icon_sites(icon_id="custom_icon", dest_icons={"generic_rep"})
    dest.failures["list_event_type_icons"] = IntegrationBadResponseError("bad gateway", status_code=502)
    assert await EventTypeSync(source, dest, types_.SETTINGS).ensure("wildlife_sighting") is not None
    assert dest.calls_to("post_event_type_v2")[0][0]["icon"] == "custom_icon"


@pytest.mark.asyncio
async def test_a_failed_cleanup_is_tried_again_the_next_day_not_every_run(world):
    from app.services.errors import IntegrationBadResponseError
    dest = dest_with_group(world)
    shared_subject(dest, "d-old", "Old", "src-gone")
    dest.failures["get_subject_sources"] = IntegrationBadResponseError("forbidden", status_code=403)
    await run_sync_observations(tracks.INTEGRATION, tracks.CONFIG)
    assert any("didn't finish" in title for title in world.published[0].errors)
    world.published.clear()
    await run_sync_observations(tracks.INTEGRATION, tracks.CONFIG)
    assert not any("didn't finish" in title for title in world.published[0].errors)


@pytest.mark.asyncio
async def test_the_summary_counts_removed_subjects(world):
    dest = dest_with_group(world)
    shared_subject(dest, "d-old", "Old", "src-gone")
    await run_sync_observations(tracks.INTEGRATION, tracks.CONFIG)
    assert ", 1 subject removed" in world.published[0].summary_title()


@pytest.mark.asyncio
async def test_a_source_error_keeps_the_subject_and_the_run_finishes(world):
    from app.services.errors import IntegrationBadResponseError
    dest = dest_with_group(world)
    shared_subject(dest, "d-old", "Old", "src-gone")
    world.source.failures["get_source"] = IntegrationBadResponseError("conflict", status_code=409)
    await run_sync_observations(tracks.INTEGRATION, tracks.CONFIG)
    assert dest.deleted_subjects == []
    assert any("couldn't show their devices" in title for title in world.published[0].errors)
    assert world.published[0].created > 0  # the points were still shared and reported


@pytest.mark.asyncio
async def test_an_unfinished_cleanup_carries_on_after_the_last_subject(world, monkeypatch):
    from app.ersharing import subject_cleanup
    dest = dest_with_group(world)
    for n in range(3):
        shared_subject(dest, f"d-{n}", f"Old {n}", f"gone-{n}")
    calls = {"n": 0}

    def out_of_time():
        calls["n"] += 1
        return calls["n"] > 1  # time for one subject per run

    from app.ersharing.reporting import DestinationReport
    from app.ersharing.state import ShareState
    state = ShareState("i1", manager=world.manager)
    for _ in range(3):
        calls["n"] = 0
        await subject_cleanup.remove_deleted_subjects(
            world.source, dest, "Sitea", SIMPLE_PROVIDER_KEY, "site:sitea.pamdas.org#",
            DestinationReport("Site 1", "Tracks"), out_of_time, state, "d1")
    assert dest.deleted_subjects == ["d-0", "d-1", "d-2"]


@pytest.mark.asyncio
async def test_an_unreadable_icon_list_leaves_an_existing_types_icon_alone():
    from app.services.errors import IntegrationBadResponseError
    source, dest = icon_sites(icon_id="custom_icon", dest_icons={"generic_rep"})
    await EventTypeSync(source, dest, types_.SETTINGS).ensure("wildlife_sighting")
    dest.failures["list_event_type_icons"] = IntegrationBadResponseError("bad gateway", status_code=502)
    await EventTypeSync(source, dest, types_.SETTINGS).ensure("wildlife_sighting")
    assert dest.event_types["sitea_wildlife_sighting"]["icon_id"] == DEFAULT_ICON
