from types import SimpleNamespace

import pytest

from app.actions import action_handlers, handlers
from app.actions.configurations import (
    AuthenticateConfig, ListShareEventTypesQuery, ListFeatureGroupsQuery,
    PreviewEventsShareQuery, PreviewSubjectsShareQuery, SyncEventsConfig, SyncObservationsConfig,
)
from app.ersharing import clients
from app.ersharing.tests.fakes import FakeSite

INTEGRATION = SimpleNamespace(
    id="i1", base_url="https://sitea.pamdas.org",
    configurations=[SimpleNamespace(action=SimpleNamespace(value="auth"), data={"token": "t"})],
)


@pytest.fixture
def source(monkeypatch):
    site = FakeSite("https://sitea.pamdas.org", role="source")
    monkeypatch.setattr(clients, "open_site", lambda url, auth, role: site)
    return site


def test_all_actions_are_registered():
    # Pull and Share actions live in one runner: one auth, one show_permissions.
    assert {"auth", "show_permissions", "pull_events", "pull_observations", "sync_events", "sync_observations",
            "list_event_types", "list_share_event_types", "list_feature_groups", "list_subject_groups",
            "preview_events_share", "preview_subjects_share"} <= set(action_handlers)


def test_sharing_actions_have_portal_names_and_descriptions():
    assert handlers.action_sync_events.action_title == "Share Events"
    assert handlers.action_sync_observations.action_title == "Share Subjects"
    # The portal shows the config's description under the section's title.
    assert SyncEventsConfig.schema()["description"].startswith("Shares this site's events")
    assert SyncObservationsConfig.schema()["description"].startswith("Shares this site's subjects")


@pytest.mark.asyncio
async def test_reference_actions_list_source_options(source):
    source.event_types = {"b": {"value": "b", "display": "Bravo", "category": {"display": "Security"}},
                          "a": {"value": "a", "display": "Alpha", "category": "monitoring"}}
    source.feature_groups = {"fg1": {"id": "fg1", "name": "Park"}}
    event_types = await handlers.action_list_share_event_types(INTEGRATION, ListShareEventTypesQuery())
    assert [(o["value"], o["label"], o["group"]) for o in event_types["options"]] == [
        ("a", "Alpha", "monitoring"), ("b", "Bravo", "Security")]
    areas = await handlers.action_list_feature_groups(INTEGRATION, ListFeatureGroupsQuery())
    assert [(o["value"], o["label"]) for o in areas["options"]] == [("fg1", "Park")]


@pytest.mark.asyncio
async def test_preview_actions_read_the_source_with_the_form_selection(source):
    source.categories = {"security": {"value": "security", "display": "Security"}}
    source.event_types = {"a": {"value": "a", "display": "Alpha", "category": "security"},
                          "b": {"value": "b", "display": "Bravo", "category": "security"}}
    source.subject_groups = [{"id": "g1", "name": "Lions", "subjects": [{"id": "s1", "name": "Leo"}]},
                             {"id": "g2", "name": "Rhinos", "subjects": [{"id": "s2", "name": "Rex"}]}]
    # The portal sends the whole form; only the selection is read.
    events = await handlers.action_preview_events_share(
        INTEGRATION, PreviewEventsShareQuery.parse_obj({"event_types": ["b"], "run_on_schedule": True}))
    assert (events["mode"], events["source_site"]) == ("selected", "https://sitea.pamdas.org")
    assert [i["value"] for g in events["groups"] for i in g["items"]] == ["b"]
    subjects = await handlers.action_preview_subjects_share(
        INTEGRATION, PreviewSubjectsShareQuery.parse_obj({"subject_groups": ["g2"], "compatibility": {}}))
    assert [g["label"] for g in subjects["groups"]] == ["Rhinos"]


def test_the_auth_form_has_no_site_url_the_connections_own_is_used():
    from app.ersharing.clients import source_url_of
    assert "base_url" not in AuthenticateConfig.schema()["properties"]
    assert source_url_of(INTEGRATION, AuthenticateConfig(token="t")) == "https://sitea.pamdas.org"