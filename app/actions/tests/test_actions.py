import pytest
from app.services.action_runner import execute_action


@pytest.mark.asyncio
async def test_execute_auth_action(
        mocker, mock_gundi_client_v2, mock_erclient_class, er_integration_v2,
        mock_publish_event
):
    mocker.patch("app.services.action_runner._portal", mock_gundi_client_v2)
    mocker.patch("app.services.activity_logger.publish_event", mock_publish_event)
    mocker.patch("app.services.action_runner.publish_event", mock_publish_event)
    mocker.patch("app.actions.handlers.AsyncERClient", mock_erclient_class)

    response = await execute_action(
        integration_id=str(er_integration_v2.id),
        action_id="auth"
    )

    assert mock_gundi_client_v2.get_integration_details.called
    assert mock_erclient_class.return_value.auth_headers.called
    assert response == {"valid_credentials": True}


@pytest.mark.asyncio
async def test_execute_pull_events_action(
        mocker, mock_gundi_client_v2, mock_state_manager, mock_erclient_class,
        mock_get_gundi_api_key, mock_gundi_sensors_client_class, er_integration_v2,
        events_batch_one, events_batch_two, mock_publish_event, mock_gundi_client_v2_class
):
    mocker.patch("app.services.activity_logger.publish_event", mock_publish_event)
    mocker.patch("app.services.action_runner.publish_event", mock_publish_event)
    mocker.patch("app.services.action_runner._portal", mock_gundi_client_v2)
    mocker.patch("app.actions.handlers.state_manager", mock_state_manager)
    mocker.patch("app.actions.handlers.AsyncERClient", mock_erclient_class)
    mocker.patch("app.services.gundi.GundiClient", mock_gundi_client_v2_class)
    mocker.patch("app.services.gundi.GundiDataSenderClient", mock_gundi_sensors_client_class)
    mocker.patch("app.services.gundi._get_gundi_api_key", mock_get_gundi_api_key)
    response = await execute_action(
        integration_id=str(er_integration_v2.id),
        action_id="pull_events"
    )

    assert mock_gundi_client_v2.get_integration_details.called
    assert mock_state_manager.get_state.called
    assert mock_state_manager.set_state.called
    assert mock_erclient_class.return_value.get_events.called
    assert mock_gundi_sensors_client_class.return_value.post_events.call_count == 2
    assert response == {"events_extracted": len(events_batch_one) + len(events_batch_two)}


@pytest.mark.asyncio
async def test_execute_pull_observations_action(
        mocker, mock_gundi_client_v2, mock_state_manager, mock_erclient_class,
        mock_get_gundi_api_key, mock_gundi_sensors_client_class, er_integration_v2,
        observations_batch_one, observations_batch_two, mock_publish_event, mock_gundi_client_v2_class
):
    mocker.patch("app.services.action_runner._portal", mock_gundi_client_v2)
    mocker.patch("app.services.activity_logger.publish_event", mock_publish_event)
    mocker.patch("app.services.action_runner.publish_event", mock_publish_event)
    mocker.patch("app.actions.handlers.state_manager", mock_state_manager)
    mocker.patch("app.actions.handlers.AsyncERClient", mock_erclient_class)
    mocker.patch("app.services.gundi.GundiClient", mock_gundi_client_v2_class)
    mocker.patch("app.services.gundi.GundiDataSenderClient", mock_gundi_sensors_client_class)
    mocker.patch("app.services.gundi._get_gundi_api_key", mock_get_gundi_api_key)
    response = await execute_action(
        integration_id=str(er_integration_v2.id),
        action_id="pull_observations"
    )

    assert mock_gundi_client_v2.get_integration_details.called
    assert mock_state_manager.get_state.called
    assert mock_state_manager.set_state.called
    assert mock_erclient_class.return_value.get_observations.called
    assert mock_gundi_sensors_client_class.return_value.post_observations.call_count == 2
    assert response == {"observations_extracted": len(observations_batch_one) + len(observations_batch_one)}
