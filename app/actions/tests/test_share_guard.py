"""Pull Events / Pull Observations stay quiet while Share is on for the integration."""
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
def pull_internals(mocker):
    """Anything past the guard: reaching it means Pull went on to run."""
    return mocker.patch.object(handlers, "get_authentication_config", side_effect=AssertionError("Pull ran"))


def test_share_actions_are_off_by_default():
    assert SyncEventsConfig().run_on_schedule is False
    assert SyncObservationsConfig().run_on_schedule is False


@pytest.mark.parametrize("share_on, expected", [
    ({}, []),
    ({"sync_events": False, "sync_observations": False}, []),
    ({"sync_events": True}, ["Share Events"]),
    ({"sync_observations": True}, ["Share Subjects"]),
    ({"sync_events": True, "sync_observations": True}, ["Share Events", "Share Subjects"]),
])
def test_scheduled_share_actions(share_on, expected):
    assert handlers._scheduled_share_actions(_integration(**share_on)) == expected


@pytest.mark.asyncio
@pytest.mark.parametrize("action, action_id, config, title", [
    (handlers.action_pull_events, "pull_events", PullEventsConfig.construct, "Pull Events"),
    (handlers.action_pull_observations, "pull_observations", PullObservationsConfig.construct, "Pull Observations"),
])
async def test_pull_skips_and_says_why_when_share_is_on(activity, pull_internals, action, action_id, config, title):
    result = await action(_integration(sync_observations=True), config())
    assert result == {"skipped": True, "reason": "share_enabled"}
    pull_internals.assert_not_called()
    kwargs = activity.await_args.kwargs
    assert kwargs["action_id"] == action_id
    assert kwargs["title"] == (
        f"{title} didn't send anything to Gundi because Share Subjects is on for this connection. "
        "Share sends this site's data straight to the EarthRanger destinations."
    )


@pytest.mark.asyncio
async def test_pull_names_both_share_actions_when_both_are_on(activity, pull_internals):
    await handlers.action_pull_events(_integration(sync_events=True, sync_observations=True), PullEventsConfig.construct())
    assert activity.await_args.kwargs["title"].startswith(
        "Pull Events didn't send anything to Gundi because Share Events and Share Subjects are on")


@pytest.mark.asyncio
async def test_pull_runs_when_share_is_off(activity, pull_internals):
    with pytest.raises(AssertionError, match="Pull ran"):
        await handlers.action_pull_events(_integration(sync_events=False), PullEventsConfig.construct())
    activity.assert_not_called()
