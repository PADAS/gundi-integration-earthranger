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
    assert handlers.action_sync_events.action_title == "ER2ER Event Sharing"
    assert handlers.action_sync_observations.action_title == "ER2ER Observation Sharing"
    # The portal shows the config's description under the section's title.
    assert SyncEventsConfig.schema()["description"] == "Shares events with another EarthRanger site."
    assert SyncObservationsConfig.schema()["description"] == "Shares observations with another EarthRanger site."
    from app.actions.configurations import PullEventsConfig, PullObservationsConfig
    assert PullEventsConfig.schema()["description"] == "Shares events with other platforms through Gundi."
    assert PullObservationsConfig.schema()["description"] == "Shares observations with other platforms through Gundi."


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


class _FakeShareState:
    markers = {}

    def __init__(self, integration_id):
        self.integration_id = integration_id

    async def get_marker(self, action_id, key):
        return self.markers.get((self.integration_id, action_id, key))


def _connection(*destinations):
    from types import SimpleNamespace
    return SimpleNamespace(destinations=[
        SimpleNamespace(id=d_id, type=SimpleNamespace(value=type_value)) for d_id, type_value in destinations])


def _gundi_returning(mocker, connection=None, error=None):
    from unittest.mock import AsyncMock, MagicMock
    from app.actions import handlers
    client = MagicMock()
    client.get_connection_details = AsyncMock(return_value=connection, side_effect=error)
    client.__aenter__ = AsyncMock(return_value=client)
    client.__aexit__ = AsyncMock(return_value=False)
    mocker.patch.object(handlers, "GundiClient", return_value=client)


@pytest.mark.asyncio
async def test_a_subject_counts_as_shared_once_every_earthranger_destination_has_it(mocker):
    from types import SimpleNamespace
    from app.actions import handlers
    from app.ersharing import state as state_module
    mocker.patch.object(state_module, "ShareState", _FakeShareState)
    _gundi_returning(mocker, _connection(("d1", "earth_ranger"), ("d2", "earth_ranger"), ("s1", "smart_connect")))
    _FakeShareState.markers = {
        ("i1", "sync_observations", "d1:subject:both"): {"ok": True},
        ("i1", "sync_observations", "d2:subject:both"): {"ok": True},
        ("i1", "sync_observations", "d1:subject:one"): {"ok": True},
    }
    is_shared = await handlers._subject_shared_check(SimpleNamespace(id="i1"))
    assert await is_shared("both") is True
    assert await is_shared("one") is False  # d2 hasn't had it yet
    assert await is_shared("none") is False


@pytest.mark.asyncio
async def test_no_shared_check_when_gundi_cant_be_read_or_has_no_earthranger_destination(mocker):
    from types import SimpleNamespace
    from app.actions import handlers
    _gundi_returning(mocker, error=RuntimeError("gundi is down"))
    assert await handlers._subject_shared_check(SimpleNamespace(id="i1")) is None
    _gundi_returning(mocker, _connection(("s1", "smart_connect")))
    assert await handlers._subject_shared_check(SimpleNamespace(id="i1")) is None
