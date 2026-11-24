"""One preview action per kind answers both forms: Pull's filters or Share's selection."""
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from app.actions import handlers
from app.actions.configurations import (
    PreviewEventsShareQuery, PreviewSubjectsShareQuery, PullEventsConfig, PullObservationsConfig,
    SyncEventsConfig, SyncObservationsConfig,
)
from app.ersharing import clients
from app.ersharing.tests.fakes import FakeSite

INTEGRATION = SimpleNamespace(
    id="i1", base_url="https://sitea.pamdas.org",
    configurations=[SimpleNamespace(action=SimpleNamespace(value="auth"), data={"token": "t"})],
)

PULL_EVENTS_FORM = {"start_datetime": "2026-06-01T00:00:00Z", "event_types": ["a"], "event_categories": ["c"],
                    "run_on_schedule": True}
SHARE_EVENTS_FORM = {"run_on_schedule": True, "event_types": ["a"], "area_feature_groups": ["fg"],
                     "compatibility": {}}
PULL_SUBJECTS_FORM = {"start_datetime": "2026-06-01T00:00:00Z", "subject_group_ids": ["g"], "run_on_schedule": True}
SHARE_SUBJECTS_FORM = {"run_on_schedule": True, "subject_groups": ["g"], "area_feature_groups": ["fg"],
                       "compatibility": {}}


def test_both_forms_of_each_kind_use_the_same_preview_action():
    assert PullEventsConfig.ui_schema()["gundi:share_preview"]["action"] == "preview_events_share"
    assert SyncEventsConfig.ui_schema()["gundi:share_preview"]["action"] == "preview_events_share"
    assert PullObservationsConfig.ui_schema()["gundi:share_preview"]["action"] == "preview_subjects_share"
    assert SyncObservationsConfig.ui_schema()["gundi:share_preview"]["action"] == "preview_subjects_share"


@pytest.mark.parametrize("model, form, from_pull", [
    (PreviewEventsShareQuery, PULL_EVENTS_FORM, True),
    (PreviewEventsShareQuery, {"start_datetime": "2026-06-01T00:00:00Z"}, True),
    (PreviewEventsShareQuery, SHARE_EVENTS_FORM, False),
    (PreviewSubjectsShareQuery, PULL_SUBJECTS_FORM, True),
    (PreviewSubjectsShareQuery, {"subject_group_ids": []}, True),
    (PreviewSubjectsShareQuery, SHARE_SUBJECTS_FORM, False),
])
def test_the_query_tells_which_form_sent_it(model, form, from_pull):
    assert model.parse_obj(form).from_pull_form is from_pull


@pytest.fixture
def pull_client(mocker):
    er_client = MagicMock()
    client_cm = MagicMock()
    client_cm.__aenter__ = AsyncMock(return_value=er_client)
    client_cm.__aexit__ = AsyncMock(return_value=False)
    mocker.patch.object(handlers, "_translating_er_client", return_value=client_cm)
    return er_client


@pytest.fixture
def share_site(monkeypatch):
    site = FakeSite("https://sitea.pamdas.org", role="source")
    monkeypatch.setattr(clients, "open_site", lambda url, auth, role: site)
    return site


@pytest.mark.asyncio
async def test_pull_events_form_gets_the_pull_preview(mocker, pull_client):
    preview = mocker.patch.object(handlers, "preview_events", AsyncMock(return_value={"mode": "selected"}))
    await handlers.action_preview_events_share(INTEGRATION, PreviewEventsShareQuery.parse_obj(PULL_EVENTS_FORM))
    preview.assert_awaited_once_with(pull_client, "https://sitea.pamdas.org", ["a"], ["c"])


@pytest.mark.asyncio
async def test_share_events_form_gets_the_share_preview(mocker, share_site):
    pull_preview = mocker.patch.object(handlers, "preview_events", AsyncMock())
    share_site.event_types = {"a": {"value": "a", "display": "Alpha", "category": "security"}}
    result = await handlers.action_preview_events_share(
        INTEGRATION, PreviewEventsShareQuery.parse_obj(SHARE_EVENTS_FORM))
    pull_preview.assert_not_called()
    assert [i["value"] for g in result["groups"] for i in g["items"]] == ["a"]


@pytest.mark.asyncio
async def test_pull_subjects_form_gets_the_pull_preview(mocker, pull_client):
    preview = mocker.patch.object(handlers, "preview_subjects", AsyncMock(return_value={"mode": "selected"}))
    await handlers.action_preview_subjects_share(
        INTEGRATION, PreviewSubjectsShareQuery.parse_obj(PULL_SUBJECTS_FORM))
    args = preview.await_args
    assert args.args == (pull_client, "https://sitea.pamdas.org", ["g"])
    assert "source_assignments" in args.kwargs


@pytest.mark.asyncio
async def test_share_subjects_form_gets_the_share_preview(mocker, share_site):
    pull_preview = mocker.patch.object(handlers, "preview_subjects", AsyncMock())
    share_site.subject_groups = [{"id": "g", "name": "Rhinos", "subjects": [{"id": "s1", "name": "Rex"}]}]
    result = await handlers.action_preview_subjects_share(
        INTEGRATION, PreviewSubjectsShareQuery.parse_obj(SHARE_SUBJECTS_FORM))
    pull_preview.assert_not_called()
    assert [g["label"] for g in result["groups"]] == ["Rhinos"]
