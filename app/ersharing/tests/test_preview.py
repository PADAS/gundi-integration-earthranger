import pytest

from app.ersharing import preview
from app.ersharing.preview import preview_events, preview_subjects
from app.ersharing.tests.fakes import FakeSite
from app.services.errors import IntegrationAuthError

SITE = "https://sitea.pamdas.org"


@pytest.fixture
def site():
    s = FakeSite(SITE, role="source")
    s.categories = {
        "security": {"value": "security", "display": "Security", "ordernum": 1},
        "monitoring": {"value": "monitoring", "display": "Monitoring", "ordernum": 2},
    }
    s.event_types = {
        "poaching": {"value": "poaching", "display": "Poaching", "category": "security",
                     "icon_id": "poaching_rep", "default_priority": 300, "is_active": True},
        "fire": {"value": "fire", "display": "Fire", "category": {"value": "monitoring"},
                 "default_priority": 200, "is_active": True},
        "old": {"value": "old", "display": "Old type", "category": "monitoring", "is_active": False},
    }
    s.subject_groups = [
        {"id": "g1", "name": "Vultures", "subjects": [
            {"id": "s1", "name": "Kali", "image_url": "/static/vulture.svg",
             "last_position_date": "2026-11-09T09:41:00+00:00"},
            {"id": "s2", "name": "Tumbo", "image_url": "https://cdn.example.org/t.svg"},
        ]},
        {"id": "g2", "name": "Elephants", "subjects": [{"id": "s1", "name": "Kali"}]},
        {"id": "g3", "name": "Empty", "subjects": []},
    ]
    return s


@pytest.mark.asyncio
async def test_all_event_types_when_none_are_picked(site):
    result = await preview_events(site, [])
    assert result["mode"] == "all"
    assert result["source_site"] == SITE
    # Every type, inactive ones too (Share Events copies their events), grouped by category.
    assert [(g["value"], [i["value"] for i in g["items"]]) for g in result["groups"]] == [
        ("security", ["poaching"]), ("monitoring", ["fire", "old"])]
    assert (result["selected_count"], result["total_count"]) == (3, 3)
    assert result["groups"][1]["items"][1]["is_active"] is False
    poaching = result["groups"][0]["items"][0]
    assert poaching == {
        "value": "poaching", "label": "Poaching",
        "icon_url": f"{SITE}/static/sprite-src/poaching_rep.svg",
        "is_active": True, "priority": 300, "detail": None,
    }
    # A type without its own icon id uses its value, as the ER web app does.
    assert result["groups"][1]["items"][0]["icon_url"] == f"{SITE}/static/sprite-src/fire.svg"
    # The group counts every type in the category, inactive ones too.
    assert result["groups"][1]["total_count"] == 2


@pytest.mark.asyncio
async def test_picked_event_types_with_missing_and_inactive_ones(site):
    result = await preview_events(site, ["old", "fire", "", None, "gone"])
    assert result["mode"] == "selected"
    assert [i["value"] for g in result["groups"] for i in g["items"]] == ["fire", "old"]
    assert [i["is_active"] for g in result["groups"] for i in g["items"]] == [True, False]
    assert result["selected_count"] == 2
    assert result["missing_values"] == ["gone"]


@pytest.mark.asyncio
async def test_v1_event_types_are_listed_too(site):
    # A site whose types are all v1: ER's v2 list is empty, yet those events
    # are shared, so the preview must list them.
    site.event_types = {}
    site.event_types_v1 = {
        "cluster": {"value": "cluster", "display": "Cluster", "category": {"value": "monitoring"},
                    "icon_id": "cluster_rep", "default_priority": 100, "is_active": True},
        "retired": {"value": "retired", "display": "Retired", "category": {"value": "monitoring"},
                    "is_active": False},
    }
    result = await preview_events(site, [])
    assert [i["value"] for g in result["groups"] for i in g["items"]] == ["cluster", "retired"]
    assert result["groups"][0]["items"][0]["icon_url"] == f"{SITE}/static/sprite-src/cluster_rep.svg"
    assert (result["selected_count"], result["total_count"]) == (2, 2)
    picked = await preview_events(site, ["retired"])
    assert picked["missing_values"] == []
    assert [i["is_active"] for g in picked["groups"] for i in g["items"]] == [False]


@pytest.mark.asyncio
async def test_a_type_on_both_lists_is_listed_once(site):
    site.event_types_v1 = {"fire": {"value": "fire", "display": "Fire (v1)", "category": {"value": "monitoring"}}}
    result = await preview_events(site, [])
    labels = [i["label"] for g in result["groups"] for i in g["items"]]
    assert labels.count("Fire") == 1 and "Fire (v1)" not in labels


@pytest.mark.asyncio
async def test_events_a_token_that_cant_read_them_gets_a_warning(site):
    site.failures["list_event_types_v2"] = IntegrationAuthError("denied", status_code=403)
    result = await preview_events(site, [])
    assert result["groups"] == []
    assert result["warning"] == "This token can't read event types on sitea.pamdas.org."


@pytest.mark.asyncio
async def test_all_subject_groups_when_none_are_picked(site):
    result = await preview_subjects(site, [])
    assert result["mode"] == "all"
    # Empty groups are left out of "all"; groups sorted by name.
    assert [g["label"] for g in result["groups"]] == ["Elephants", "Vultures"]
    # Distinct subjects: Kali is in two groups and shared once.
    assert (result["selected_count"], result["total_count"]) == (2, 2)
    vultures = result["groups"][1]
    assert vultures["items"][0] == {
        "value": "s1", "label": "Kali", "icon_url": f"{SITE}/static/vulture.svg",
        "is_active": True, "detail": "2026-11-09T09:41:00+00:00", "note": preview.OLD_TRACK_NOTE,
    }
    assert vultures["items"][1]["icon_url"] == "https://cdn.example.org/t.svg"
    assert vultures["items"][1]["detail"] is None
    assert vultures["items"][1]["note"] is None  # no track: the portal already says so


@pytest.mark.asyncio
async def test_a_subject_with_a_recent_point_has_no_note(site, monkeypatch):
    from app.ersharing.timeutil import parse_dt
    monkeypatch.setattr(preview, "utcnow", lambda: parse_dt("2026-11-09T20:00:00Z"))
    result = await preview_subjects(site, ["g1"])
    assert result["groups"][0]["items"][0]["note"] is None  # 10 hours old: inside the window
    monkeypatch.setattr(preview, "utcnow", lambda: parse_dt("2026-11-09T22:00:00Z"))
    result = await preview_subjects(site, ["g1"])
    assert result["groups"][0]["items"][0]["note"] == preview.OLD_TRACK_NOTE


def test_the_note_uses_the_first_share_window():
    from app.ersharing import sync_observations
    assert preview.FIRST_SHARE_WINDOW == sync_observations.BACKFILL


@pytest.mark.asyncio
async def test_picked_subject_groups_keep_empty_ones_and_name_missing_ones(site):
    result = await preview_subjects(site, ["g3", "nope"])
    assert [(g["label"], g["total_count"]) for g in result["groups"]] == [("Empty", 0)]
    assert result["selected_count"] == 0
    assert result["missing_values"] == ["nope"]


@pytest.mark.asyncio
async def test_subjects_a_token_that_cant_read_groups_gets_a_warning(site):
    site.failures["list_subject_groups"] = IntegrationAuthError("denied", status_code=403)
    result = await preview_subjects(site, [])
    assert result["groups"] == []
    assert result["warning"] == "This token can't read subject groups on sitea.pamdas.org."


@pytest.mark.asyncio
async def test_long_groups_are_cut_and_say_so(site, monkeypatch):
    monkeypatch.setattr(preview, "ITEM_CAP", 1)
    result = await preview_subjects(site, ["g1"])
    assert result["truncated"] is True
    assert len(result["groups"][0]["items"]) == 1
    assert result["groups"][0]["total_count"] == 2


@pytest.mark.asyncio
async def test_a_type_whose_category_isnt_listed_still_shows(site):
    site.event_types_v1 = {"snare": {
        "value": "snare", "display": "Snare", "is_active": True,
        "category": {"value": "hidden_cat", "display": "Hidden category", "ordernum": 9},
    }}
    result = await preview_events(site, ["snare"])
    assert [(g["label"], [i["value"] for i in g["items"]]) for g in result["groups"]] == [
        ("Hidden category", ["snare"])]
    assert result["missing_values"] == []


@pytest.mark.asyncio
async def test_picked_values_are_matched_exactly_as_stored(site):
    # The sharing action compares stored values as they are, so the preview does too.
    result = await preview_events(site, ["fire ", "fire"])
    assert [i["value"] for g in result["groups"] for i in g["items"]] == ["fire"]
    assert result["missing_values"] == ["fire "]


@pytest.mark.asyncio
async def test_a_rejected_token_is_named_as_such(site):
    site.failures["get_event_categories"] = IntegrationAuthError("rejected", status_code=401)
    result = await preview_events(site, [])
    assert result["warning"] == "EarthRanger rejected this token on sitea.pamdas.org."


@pytest.mark.asyncio
async def test_event_types_are_listed_without_their_schemas(site):
    await preview_events(site, [])
    assert site.v2_list_include_schema is False


@pytest.mark.asyncio
async def test_inactive_v2_types_are_read_so_a_picked_one_isnt_missing(site):
    result = await preview_events(site, ["old"])
    assert site.v2_list_include_inactive is True
    assert result["missing_values"] == []


@pytest.mark.asyncio
async def test_an_area_filter_is_noted(site):
    events = await preview_events(site, [], ["fg1"])
    assert events["warning"] == "Only events inside the chosen areas are shared."
    tracks = await preview_subjects(site, [], ["fg1", ""])
    assert tracks["warning"] == "Only track points inside the chosen areas are shared."
    assert (await preview_subjects(site, [], [""]))["warning"] is None



def _point(lon, lat):
    return {"geometry": {"type": "Point", "coordinates": [lon, lat]}}


@pytest.mark.asyncio
async def test_each_subject_says_whether_its_last_point_is_inside_the_area(site, monkeypatch):
    from app.ersharing.timeutil import parse_dt
    monkeypatch.setattr(preview, "utcnow", lambda: parse_dt("2026-11-09T12:00:00Z"))
    site.feature_groups["fg1"] = {"id": "fg1", "features": [{"features": [{"type": "Feature", "geometry": {
        "type": "Polygon", "coordinates": [[[5, 5], [5, 6], [6, 6], [6, 5], [5, 5]]]}}]}]}
    site.subject_groups = [{"id": "g1", "name": "Vultures", "subjects": [
        {"id": "s1", "name": "Inside", "last_position": _point(5.5, 5.5),
         "last_position_date": "2026-11-09T11:00:00+00:00"},
        {"id": "s2", "name": "Outside", "last_position": _point(7, 7),
         "last_position_date": "2026-11-09T11:00:00+00:00"},
        {"id": "s3", "name": "Old inside", "last_position": _point(5.2, 5.8),
         "last_position_date": "2026-11-01T11:00:00+00:00"},
        {"id": "s4", "name": "No track"},
    ]}]
    result = await preview_subjects(site, [], ["fg1"])
    notes = {item["label"]: item["note"] for item in result["groups"][0]["items"]}
    assert notes == {
        "Inside": preview.INSIDE_AREA_NOTE,
        "Outside": preview.OUTSIDE_AREA_NOTE,
        "Old inside": f"{preview.INSIDE_AREA_NOTE} {preview.OLD_TRACK_NOTE}",
        "No track": None,
    }
    assert result["warning"] == preview.AREA_NOTE_TRACKS
    assert (await preview_subjects(site, [], []))["groups"][0]["items"][0]["note"] is None


@pytest.mark.asyncio
async def test_an_area_that_cant_be_read_leaves_the_list_without_area_notes(site):
    site.subject_groups[0]["subjects"][1]["last_position"] = _point(5.5, 5.5)
    result = await preview_subjects(site, [], ["missing-group"])
    assert result["groups"][1]["items"][1]["note"] is None
    assert result["warning"] == preview.AREA_NOTE_TRACKS

@pytest.mark.asyncio
async def test_types_without_a_category_get_their_own_group_last(site):
    site.event_types["loose"] = {"value": "loose", "display": "Loose report", "category": None, "is_active": True}
    site.event_types["blank"] = {"value": "blank", "display": "Blank report", "category": "", "is_active": True}
    result = await preview_events(site, [])
    assert [(g["label"], [i["value"] for i in g["items"]]) for g in result["groups"]] == [
        ("Security", ["poaching"]), ("Monitoring", ["fire", "old"]), ("Uncategorized", ["blank", "loose"])]
    assert (result["selected_count"], result["total_count"]) == (5, 5)
    assert result["groups"][-1]["total_count"] == 2
