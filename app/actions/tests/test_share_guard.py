"""Pull Events / Pull Observations stay quiet while the Share action for the same data is on."""
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from app.actions import handlers
from app.actions.configurations import PullEventsConfig, PullObservationsConfig, SyncEventsConfig, SyncObservationsConfig


def _integration(**share_on):
    configurations = [SimpleNamespace(action=SimpleNamespace(value="auth"), data={"token": "t"})]
    for action_id, on in share_on.items():
        configurations.append(SimpleNamespace(action=SimpleNamespace(value=action_id), data={"run_on_schedule": on}))
    return SimpleNamespace(id="i1", base_url="https://sitea.pamdas.org", configurations=configurations)


@pytest.fixture
def activity(mocker):
    mocker.patch("app.services.activity_logger.publish_event", AsyncMock())
    log = AsyncMock()
    mocker.patch.object(handlers, "log_action_activity", log)
    return log


@pytest.fixture
def pull_state(mocker):
    """The Pull action's saved state, read and written by the guard."""
    saved = {"state": {"last_execution": "2026-11-01T00:00:00+00:00"}}
    get = AsyncMock(side_effect=lambda **kwargs: dict(saved["state"]))

    async def set_state(**kwargs):
        saved["state"] = kwargs["state"]

    mocker.patch.object(handlers.state_manager, "get_state", get)
    mocker.patch.object(handlers.state_manager, "set_state", AsyncMock(side_effect=set_state))
    return saved


@pytest.fixture
def pull_internals(mocker):
    """Anything past the guard: reaching it means Pull went on to run."""
    return mocker.patch.object(handlers, "get_authentication_config", side_effect=AssertionError("Pull ran"))


def test_share_actions_are_off_by_default():
    assert SyncEventsConfig().run_on_schedule is False
    assert SyncObservationsConfig().run_on_schedule is False


@pytest.mark.parametrize("share_on, pull_action_id, expected", [
    ({}, "pull_events", None),
    ({"sync_events": False}, "pull_events", None),
    ({"sync_events": True}, "pull_events", "ER2ER Event Sharing"),
    ({"sync_observations": True}, "pull_events", None),
    ({"sync_observations": True}, "pull_observations", "ER2ER Observation Sharing"),
    ({"sync_events": True}, "pull_observations", None),
])
def test_each_pull_follows_the_share_action_for_the_same_data(share_on, pull_action_id, expected):
    assert handlers._share_is_on(_integration(**share_on), pull_action_id) == expected


@pytest.mark.asyncio
@pytest.mark.parametrize("action, action_id, config, title, share_action_id, share_title", [
    (handlers.action_pull_events, "pull_events", PullEventsConfig.construct, "Share Events from EarthRanger",
     "sync_events", "ER2ER Event Sharing"),
    (handlers.action_pull_observations, "pull_observations", PullObservationsConfig.construct, "Share Observations from EarthRanger",
     "sync_observations", "ER2ER Observation Sharing"),
])
async def test_pull_skips_and_says_why_when_its_share_action_is_on(
        activity, pull_state, pull_internals, action, action_id, config, title, share_action_id, share_title):
    result = await action(_integration(**{share_action_id: True}), config())
    assert result == {"skipped": True, "reason": "share_enabled"}
    pull_internals.assert_not_called()
    kwargs = activity.await_args.kwargs
    assert kwargs["action_id"] == action_id
    assert kwargs["title"] == (
        f"{title} didn't send anything to Gundi because {share_title} is on for this connection. "
        f"{share_title} sends this site's data straight to the EarthRanger destinations."
    )


@pytest.mark.asyncio
async def test_a_skipped_pull_moves_its_watermark_to_now(activity, pull_state, pull_internals):
    await handlers.action_pull_events(_integration(sync_events=True), PullEventsConfig.construct())
    assert set(pull_state["state"]) == {"last_execution"}
    assert pull_state["state"]["last_execution"] > "2026-11-01T00:00:00+00:00"


@pytest.mark.asyncio
async def test_a_skipped_pull_keeps_an_unfinished_observations_backfill(activity, pull_state, pull_internals):
    unfinished = {"last_execution": "2026-10-01T00:00:00+00:00", "backfill": {"start": "2026-10-01", "window_index": 3}}
    pull_state["state"] = dict(unfinished)
    await handlers.action_pull_observations(_integration(sync_observations=True), PullObservationsConfig.construct())
    assert pull_state["state"] == unfinished


@pytest.mark.asyncio
@pytest.mark.parametrize("share_on", [{"sync_events": False}, {"sync_observations": True}])
async def test_pull_events_runs_when_share_events_is_off(activity, pull_state, pull_internals, share_on):
    with pytest.raises(AssertionError, match="Pull ran"):
        await handlers.action_pull_events(_integration(**share_on), PullEventsConfig.construct())
    activity.assert_not_called()
