import json
from unittest.mock import AsyncMock, MagicMock

import pytest
from erclient import VERSION_1_0, VERSION_2_0

from app.actions.configurations import (
    PreviewEventsShareQuery,
    PreviewSubjectsShareQuery,
    PullEventsConfig,
    PullObservationsConfig,
)
from app.actions.share_preview import ITEM_CAP, preview_events, preview_subjects
from app.actions import handlers

SITE = "https://site.pamdas.org"

V2_TYPES = [
    {"value": "cmore_1_poacher", "display": "Evidence of Poacher", "category": "cmore_1",
     "icon_id": "cmore_1_poacher", "default_priority": 300, "is_active": True},
    {"value": "cmore_1_trade", "display": "Pangolin Trade", "category": "cmore_1",
     "icon_id": None, "default_priority": 0, "is_active": True},
    {"value": "cmore_1_old", "display": "Old Type", "category": "cmore_1", "is_active": False},
    {"value": "wildlife_sighting_rep", "display": "Wildlife Sighting", "category": "monitoring",
     "icon_id": "wildlife_sighting_rep", "default_priority": 100, "is_active": True},
]
V1_TYPES = [
    {"value": "carcass_rep", "display": "Carcass", "is_active": True, "default_priority": 200,
     "category": {"value": "security", "display": "Security", "id": "c-sec"}},
]
CATEGORIES = [
    {"id": "c-cmore", "value": "cmore_1", "display": "Cmore 1", "ordernum": 2},
    {"id": "c-mon", "value": "monitoring", "display": "Monitoring", "ordernum": 1},
]


def _er_client(types_v2=V2_TYPES, types_v1=V1_TYPES, categories=CATEGORIES, subjectgroups=None):
    client = MagicMock()

    async def get_event_types(include_inactive=False, include_schema=False, version=None):
        return {VERSION_2_0: types_v2, VERSION_1_0: types_v1}[version]

    client.get_event_types = AsyncMock(side_effect=get_event_types)
    client.get_event_categories = AsyncMock(return_value=categories)
    client.get_subjectgroups = AsyncMock(return_value=subjectgroups or [])
    return client


@pytest.mark.asyncio
async def test_events_preview_lists_only_types_that_pass_both_filters():
    result = await preview_events(_er_client(), SITE, ["cmore_1_poacher", "cmore_1_trade"], ["cmore_1"])

    assert result["mode"] == "selected"
    assert result["selected_count"] == 2
    assert [g["label"] for g in result["groups"]] == ["Cmore 1"]
    items = result["groups"][0]["items"]
    assert [i["label"] for i in items] == ["Evidence of Poacher", "Pangolin Trade"]
    assert items[0]["priority"] == 300
    assert items[0]["icon_url"] == f"{SITE}/static/sprite-src/cmore_1_poacher.svg"
    # No icon_id: the slug names the icon, as in EarthRanger.
    assert items[1]["icon_url"] == f"{SITE}/static/sprite-src/cmore_1_trade.svg"
    assert result["missing_values"] == [] and result["warning"] is None


@pytest.mark.asyncio
async def test_events_preview_flags_missing_values_and_types_outside_the_categories():
    result = await preview_events(
        _er_client(), SITE, ["cmore_1_poacher", "wildlife_sighting_rep", "nope_rep"], ["cmore_1", "gone"]
    )

    assert result["selected_count"] == 1
    assert result["missing_values"] == ["nope_rep", "gone"]
    assert "Wildlife Sighting" in result["warning"]


@pytest.mark.asyncio
async def test_events_preview_category_only_lists_its_types_inactive_included():
    # pull_events adds no active-type constraint, so an inactive type's events are sent.
    result = await preview_events(_er_client(), SITE, [], ["cmore_1"])

    items = [i for g in result["groups"] for i in g["items"]]
    assert [i["label"] for i in items] == ["Evidence of Poacher", "Old Type", "Pangolin Trade"]
    assert [i["is_active"] for i in items] == [True, False, True]
    assert result["groups"][0]["total_count"] == 3


@pytest.mark.asyncio
async def test_events_preview_drops_an_inactive_type_picked_by_name():
    # pull_events resolves picked types from the active listings only.
    result = await preview_events(_er_client(), SITE, ["cmore_1_old", "cmore_1_trade"], [])

    assert [i["value"] for g in result["groups"] for i in g["items"]] == ["cmore_1_trade"]
    assert "inactive" in result["warning"] and "Old Type" in result["warning"]


@pytest.mark.asyncio
async def test_events_preview_drops_a_category_the_pull_cant_resolve():
    # A v2 type's category missing from the categories answer has no UUID, so
    # the pull can't filter by it and skips: nothing is listed.
    types = V2_TYPES + [{"value": "x_rep", "display": "X", "category": "ghost", "is_active": True}]
    result = await preview_events(_er_client(types_v2=types), SITE, [], ["ghost"])

    assert result["groups"] == [] and result["selected_count"] == 0
    assert result["missing_values"] == ["ghost"]


@pytest.mark.asyncio
async def test_events_preview_lists_types_without_a_category_as_uncategorized():
    types = V2_TYPES + [{"value": "loose_rep", "display": "Loose", "category": None, "is_active": True}]
    result = await preview_events(_er_client(types_v2=types), SITE, ["loose_rep"], [])

    assert [(g["label"], [i["value"] for i in g["items"]]) for g in result["groups"]] == [
        ("Uncategorized", ["loose_rep"])
    ]
    # The runner answers with JSON that refuses NaN/Infinity.
    json.dumps(result, allow_nan=False)

    everything = await preview_events(_er_client(types_v2=types), SITE, [], [])
    assert everything["groups"][-1]["label"] == "Uncategorized"
    json.dumps(everything, allow_nan=False)


@pytest.mark.asyncio
async def test_events_preview_resolves_categories_only_through_active_v1_types():
    # Categories call down: pull_events resolves a category only from the
    # active v1 listing, so a category reachable only through an inactive v1
    # type makes it skip.
    v1 = [{"value": "old_rep", "display": "Old", "is_active": False,
           "category": {"value": "legacy", "display": "Legacy", "id": "c-legacy"}}]
    client = _er_client(types_v1=v1)
    client.get_event_categories = AsyncMock(side_effect=RuntimeError("down"))
    with pytest.raises(RuntimeError):
        await preview_events(client, SITE, [], ["legacy"])


@pytest.mark.asyncio
async def test_events_preview_with_no_filter_lists_every_active_type_v1_included():
    result = await preview_events(_er_client(), SITE, [], [])

    assert result["mode"] == "all"
    assert result["total_count"] == 5
    # Ordered by category ordernum; v1's nested category becomes a group.
    assert [g["label"] for g in result["groups"]] == ["Security", "Monitoring", "Cmore 1"]


@pytest.mark.asyncio
async def test_events_preview_warns_when_v1_types_are_missing_and_survives_a_category_failure():
    client = _er_client()

    async def get_event_types(include_inactive=False, include_schema=False, version=None):
        if version == VERSION_1_0:
            raise RuntimeError("v1 down")
        return V2_TYPES

    client.get_event_types = AsyncMock(side_effect=get_event_types)
    client.get_event_categories = AsyncMock(side_effect=RuntimeError("down"))
    result = await preview_events(client, SITE, ["cmore_1_poacher"], [])

    # Without category names, the group is named by its slug.
    assert result["groups"][0]["label"] == "cmore_1"
    assert result["selected_count"] == 1
    assert "classic (v1)" in result["warning"]


@pytest.mark.asyncio
async def test_events_preview_fails_when_a_picked_category_cant_be_resolved():
    # v2 types carry the category as a bare slug, so without the categories
    # call pull_events can't resolve cmore_1 to its UUID and skips the pull.
    client = _er_client()
    client.get_event_categories = AsyncMock(side_effect=RuntimeError("down"))
    with pytest.raises(RuntimeError):
        await preview_events(client, SITE, [], ["cmore_1"])

    # A category v1 types already resolve is still previewed.
    result = await preview_events(client, SITE, [], ["security"])
    assert result["groups"][0]["items"][0]["value"] == "carcass_rep"


@pytest.mark.asyncio
async def test_events_preview_says_when_er_sent_only_part_of_a_list():
    client = _er_client(types_v2={"results": V2_TYPES, "next": "https://site/api?page=2"})
    result = await preview_events(client, SITE, [], [])

    assert result["truncated"] is True
    assert "only part of this list" in result["warning"]


SUBJECT_TREE = [
    {"id": "g-parent", "name": "Parent", "subjects": [
        {"id": "s1", "name": "Zebra", "image_url": "/static/zebra.svg", "last_position_date": "2026-11-09T09:41:00Z"},
    ], "subgroups": [
        {"id": "g-child", "name": "Child", "subjects": [
            {"id": "s2", "name": "Lion", "image_url": "https://cdn/lion.svg"},
            {"id": "s1", "name": "Zebra", "image_url": "/static/zebra.svg"},
        ], "subgroups": []},
    ]},
    {"id": "g-empty", "name": "Empty", "subjects": [], "subgroups": []},
]


@pytest.mark.asyncio
async def test_subjects_preview_includes_subgroups_of_a_picked_group():
    client = _er_client(subjectgroups=SUBJECT_TREE)
    result = await preview_subjects(client, SITE, ["g-parent", "missing"])

    client.get_subjectgroups.assert_awaited_once_with(flat=False)
    assert result["mode"] == "selected"
    group = result["groups"][0]
    assert group["label"] == "Parent" and group["total_count"] == 2
    assert [i["label"] for i in group["items"]] == ["Lion", "Zebra"]
    zebra = group["items"][1]
    assert zebra["icon_url"] == f"{SITE}/static/zebra.svg"
    assert zebra["detail"] == "2026-11-09T09:41:00Z"
    assert group["items"][0]["icon_url"] == "https://cdn/lion.svg"
    assert result["selected_count"] == 2
    assert result["missing_values"] == ["missing"]
    assert "devices these subjects use now" in result["warning"]


@pytest.mark.asyncio
async def test_subjects_preview_leaves_out_subjects_without_a_device():
    async def assignments(subject_ids):
        assert subject_ids == ["s1", "s2"]
        return [{"subject": "s2", "source": "src-2"}], True

    result = await preview_subjects(_er_client(subjectgroups=SUBJECT_TREE), SITE, ["g-parent"], assignments)

    assert [i["label"] for g in result["groups"] for i in g["items"]] == ["Lion"]
    assert result["selected_count"] == 1
    assert "1 subject in these groups has no device" in result["warning"]
    json.dumps(result, allow_nan=False)


@pytest.mark.asyncio
async def test_subjects_preview_with_no_group_lists_every_group_with_subjects_once():
    result = await preview_subjects(_er_client(subjectgroups=SUBJECT_TREE), SITE, [])

    assert result["mode"] == "all"
    assert "aren't in any subject group" in result["warning"]
    assert [g["label"] for g in result["groups"]] == ["Child", "Parent"]
    assert result["selected_count"] == result["total_count"] == 2


@pytest.mark.asyncio
async def test_subjects_preview_cuts_long_groups_and_keeps_the_exact_count():
    subjects = [{"id": f"s{i}", "name": f"S{i:04}"} for i in range(ITEM_CAP + 5)]
    tree = [{"id": "g", "name": "Big", "subjects": subjects, "subgroups": []}]
    result = await preview_subjects(_er_client(subjectgroups=tree), SITE, ["g"])

    assert result["truncated"] is True
    assert len(result["groups"][0]["items"]) == ITEM_CAP
    assert result["groups"][0]["total_count"] == ITEM_CAP + 5



@pytest.mark.asyncio
async def test_blank_entries_filter_like_the_pull_instead_of_meaning_everything():
    events = await preview_events(_er_client(), SITE, [""], None)
    assert events["mode"] == "selected" and events["selected_count"] == 0
    assert "Pull Events skips every run" in events["warning"]

    subjects = await preview_subjects(_er_client(subjectgroups=SUBJECT_TREE), SITE, [" "])
    assert subjects["mode"] == "selected" and subjects["selected_count"] == 0
    assert "Pull Observations skips every run" in subjects["warning"]


@pytest.mark.asyncio
async def test_events_preview_says_the_pull_skips_when_no_picked_type_resolves():
    result = await preview_events(_er_client(), SITE, ["cmore_1_old", "nope"], None)
    assert result["selected_count"] == 0
    assert "Pull Events skips every run" in result["warning"]

    fine = await preview_events(_er_client(), SITE, ["cmore_1_poacher"], None)
    assert "skips every run" not in (fine["warning"] or "")


@pytest.mark.asyncio
async def test_subjects_preview_says_the_pull_skips_when_no_picked_subject_has_a_device():
    async def assignments(subject_ids):
        return [], True

    result = await preview_subjects(_er_client(subjectgroups=SUBJECT_TREE), SITE, ["g-parent"], assignments)
    assert result["selected_count"] == 0
    assert "Pull Observations skips every run" in result["warning"]


@pytest.mark.asyncio
async def test_subjects_preview_checks_devices_with_no_group_picked_too():
    async def assignments(subject_ids):
        return [{"subject": "s2", "source": "src-2"}], True

    result = await preview_subjects(_er_client(subjectgroups=SUBJECT_TREE), SITE, [], assignments)
    assert result["mode"] == "all"
    assert [i["label"] for g in result["groups"] for i in g["items"]] == ["Lion"]
    assert result["selected_count"] == 1
    assert "1 subject has no device" in result["warning"]
    assert "skips every run" not in result["warning"]


@pytest.mark.asyncio
async def test_subjects_preview_marks_an_incomplete_device_lookup():
    async def assignments(subject_ids):
        return [{"subject": "s2", "source": "src-2"}], False

    result = await preview_subjects(_er_client(subjectgroups=SUBJECT_TREE), SITE, ["g-parent"], assignments)
    assert result["truncated"] is True
    assert "sent only part of this list" in result["warning"]


@pytest.mark.asyncio
async def test_preview_device_lookup_reads_chunks_at_once_and_reports_gaps():
    import asyncio
    running = peak = 0

    async def get_source_assignments(subject_ids):
        nonlocal running, peak
        running += 1
        peak = max(peak, running)
        await asyncio.sleep(0)
        running -= 1
        if subject_ids[0] == "s0":
            return {"results": [{"subject": "s0", "source": "src-0"}], "next": "page2"}
        return [{"subject": s, "source": f"src-{s}"} for s in subject_ids]

    client = MagicMock()
    client.get_source_assignments = AsyncMock(side_effect=get_source_assignments)
    ids = [f"s{i}" for i in range(handlers.SUBJECT_ID_CHUNK_SIZE * 4)]

    found, complete = await handlers._preview_source_assignments(client, ids)

    assert client.get_source_assignments.await_count == 4
    assert peak > 1
    assert complete is False
    assert len(found) == 1 + handlers.SUBJECT_ID_CHUNK_SIZE * 3


def test_pull_actions_ask_the_portal_for_the_preview():
    assert PullEventsConfig.ui_schema()["gundi:share_preview"] == {
        "action": "preview_events_share", "kind": "events",
        "selection_fields": ["event_types", "event_categories"],
    }
    assert PullObservationsConfig.ui_schema()["gundi:share_preview"] == {
        "action": "preview_subjects_share", "kind": "subjects",
        "selection_fields": ["subject_group_ids"],
    }


def test_preview_queries_ignore_the_rest_of_the_form():
    form = {"start_datetime": "2026-06-01T00:00:00Z", "run_on_schedule": True,
            "event_types": ["a"], "event_categories": ["b"], "subject_group_ids": ["g"]}
    assert PreviewEventsShareQuery.parse_obj(form).event_types == ["a"]
    assert PreviewSubjectsShareQuery.parse_obj(form).subject_group_ids == ["g"]


@pytest.mark.asyncio
async def test_list_subject_groups_offers_every_group_by_name_under_its_parent(mocker, er_integration_v2_provider):
    from app.actions import handlers as handlers_module
    from app.actions.configurations import ListSubjectGroupsQuery

    client = _er_client(subjectgroups=SUBJECT_TREE)
    client_cls = MagicMock()
    client_cls.return_value.__aenter__ = AsyncMock(return_value=client)
    client_cls.return_value.__aexit__ = AsyncMock(return_value=False)
    mocker.patch.object(handlers_module, "AsyncERClient", client_cls)

    result = await handlers_module.action_list_subject_groups(er_integration_v2_provider, ListSubjectGroupsQuery())

    client.get_subjectgroups.assert_awaited_once_with(flat=False)
    options = {o["value"]: o for o in result["options"]}
    assert set(options) == {"g-parent", "g-child", "g-empty"}
    assert options["g-child"]["label"] == "Child" and options["g-child"]["group"] == "Parent"
    assert options["g-parent"]["group"] is None
    # A parent counts its subgroups' subjects, as picking it includes them.
    assert options["g-parent"]["description"] == "2 subjects"
    assert options["g-child"]["description"] == "2 subjects"
    assert result["truncated"] is False

    client.get_subjectgroups = AsyncMock(return_value={"results": SUBJECT_TREE, "next": "https://site/api?page=2"})
    result = await handlers_module.action_list_subject_groups(er_integration_v2_provider, ListSubjectGroupsQuery())
    assert result["truncated"] is True


def test_subject_groups_field_is_a_dropdown_of_the_site_groups():
    ui = PullObservationsConfig.ui_schema()["subject_group_ids"]
    assert ui["items"]["gundi:reference"]["action"] == "list_subject_groups"
