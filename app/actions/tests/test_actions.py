import pytest

from app.conftest import async_return
from app.services.action_runner import execute_action


@pytest.mark.asyncio
async def test_execute_auth_action_with_valid_credentials(
        mocker, mock_gundi_client_v2, mock_erclient_class, er_integration_v2_provider,
        mock_publish_event, mock_config_manager_er_provider
):
    mocker.patch("app.services.action_runner._portal", mock_gundi_client_v2)
    mocker.patch("app.services.activity_logger.publish_event", mock_publish_event)
    mocker.patch("app.services.action_runner.publish_event", mock_publish_event)
    mocker.patch("app.services.action_runner.config_manager", mock_config_manager_er_provider)
    mocker.patch("app.actions.handlers.AsyncERClient", mock_erclient_class)

    response = await execute_action(
        integration_id=str(er_integration_v2_provider.id),
        action_id="auth"
    )

    assert mock_config_manager_er_provider.get_integration_details.called
    assert mock_erclient_class.return_value.get_me.called
    assert response.get("valid_credentials") == True


@pytest.mark.parametrize(
    "mock_erclient_class_with_error",
    [
        "er_401_exception",
        "er_500_exception",
        "er_generic_exception",
        "er_connect_error",
        "er_read_timeout_error",
    ],
    indirect=["mock_erclient_class_with_error"])
@pytest.mark.asyncio
async def test_execute_auth_action_with_invalid_credentials(
        mocker, mock_gundi_client_v2, er_integration_v2_provider,
        mock_publish_event, mock_erclient_class_with_error, mock_config_manager_er_provider
):
    mocker.patch("app.services.action_runner._portal", mock_gundi_client_v2)
    mocker.patch("app.services.activity_logger.publish_event", mock_publish_event)
    mocker.patch("app.services.action_runner.publish_event", mock_publish_event)
    mocker.patch("app.services.action_runner.config_manager", mock_config_manager_er_provider)
    mocker.patch("app.actions.handlers.AsyncERClient", mock_erclient_class_with_error)

    response = await execute_action(
        integration_id=str(er_integration_v2_provider.id),
        action_id="auth"
    )

    assert mock_config_manager_er_provider.get_integration_details.called
    assert mock_erclient_class_with_error.return_value.get_me.called
    assert response.get("valid_credentials") == False
    assert "error" in response


@pytest.mark.asyncio
async def test_execute_pull_events_action(
        mocker, mock_gundi_client_v2, mock_state_manager, mock_erclient_class,
        mock_get_gundi_api_key, mock_gundi_sensors_client_class, er_integration_v2_provider,
        events_batch_one, events_batch_two, mock_publish_event, mock_gundi_client_v2_class,
        mock_config_manager_er_provider
):
    mocker.patch("app.services.activity_logger.publish_event", mock_publish_event)
    mocker.patch("app.services.action_runner.publish_event", mock_publish_event)
    mocker.patch("app.services.action_runner.config_manager", mock_config_manager_er_provider)
    mocker.patch("app.services.action_runner._portal", mock_gundi_client_v2)
    mocker.patch("app.actions.handlers.state_manager", mock_state_manager)
    mocker.patch("app.actions.handlers.AsyncERClient", mock_erclient_class)
    mocker.patch("app.services.gundi.GundiClient", mock_gundi_client_v2_class)
    mocker.patch("app.services.gundi.GundiDataSenderClient", mock_gundi_sensors_client_class)
    mocker.patch("app.services.gundi._get_gundi_api_key", mock_get_gundi_api_key)
    response = await execute_action(
        integration_id=str(er_integration_v2_provider.id),
        action_id="pull_events"
    )

    assert mock_config_manager_er_provider.get_integration_details.called
    assert mock_state_manager.get_state.called
    assert mock_state_manager.set_state.called
    assert mock_erclient_class.return_value.get_events.called
    assert mock_gundi_sensors_client_class.return_value.post_events.call_count == 2
    assert response == {"events_extracted": len(events_batch_one) + len(events_batch_two)}


@pytest.mark.asyncio
async def test_execute_pull_observations_action(
        mocker, mock_gundi_client_v2, mock_state_manager, mock_erclient_class,
        mock_get_gundi_api_key, mock_gundi_sensors_client_class, er_integration_v2_provider,
        observations_batch_one, observations_batch_two, mock_publish_event, mock_gundi_client_v2_class,
        mock_config_manager_er_provider
):
    mocker.patch("app.services.action_runner._portal", mock_gundi_client_v2)
    mocker.patch("app.services.activity_logger.publish_event", mock_publish_event)
    mocker.patch("app.services.action_runner.publish_event", mock_publish_event)
    mocker.patch("app.services.action_runner.config_manager", mock_config_manager_er_provider)
    mocker.patch("app.actions.handlers.state_manager", mock_state_manager)
    mocker.patch("app.actions.handlers.AsyncERClient", mock_erclient_class)
    mocker.patch("app.services.gundi.GundiClient", mock_gundi_client_v2_class)
    mocker.patch("app.services.gundi.GundiDataSenderClient", mock_gundi_sensors_client_class)
    mocker.patch("app.services.gundi._get_gundi_api_key", mock_get_gundi_api_key)
    response = await execute_action(
        integration_id=str(er_integration_v2_provider.id),
        action_id="pull_observations"
    )

    assert mock_config_manager_er_provider.get_integration_details.called
    assert mock_state_manager.get_state.called
    assert mock_state_manager.set_state.called
    assert mock_erclient_class.return_value.get_observations.called
    assert mock_gundi_sensors_client_class.return_value.post_observations.call_count == 2
    assert response == {"observations_extracted": len(observations_batch_one) + len(observations_batch_one)}


@pytest.mark.asyncio
async def test_execute_auth_action_with_invalid_url(
        mocker, mock_gundi_client_v2, mock_erclient_class,
        er_integration_v2_with_empty_url, mock_publish_event, mock_config_manager_er_provider
):
    mock_config_manager_er_provider.get_integration_details.return_value = async_return(
        er_integration_v2_with_empty_url
    )
    mocker.patch("app.services.action_runner._portal", mock_gundi_client_v2)
    mocker.patch("app.services.activity_logger.publish_event", mock_publish_event)
    mocker.patch("app.services.action_runner.publish_event", mock_publish_event)
    mocker.patch("app.services.action_runner.config_manager", mock_config_manager_er_provider)
    mocker.patch("app.actions.handlers.AsyncERClient", mock_erclient_class)

    response = await execute_action(
        integration_id=str(er_integration_v2_with_empty_url.id),
        action_id="auth"
    )

    assert mock_config_manager_er_provider.get_integration_details.called
    assert not mock_erclient_class.return_value.get_me.called
    assert response.get("valid_credentials") == False
    assert "error" in response


@pytest.mark.asyncio
async def test_execute_auth_action_with_empty_token(
        mocker, mock_gundi_client_v2, mock_erclient_class,
        er_integration_v2_provider, mock_publish_event, mock_config_manager_er_provider
):
    mocker.patch("app.services.action_runner._portal", mock_gundi_client_v2)
    mocker.patch("app.services.activity_logger.publish_event", mock_publish_event)
    mocker.patch("app.services.action_runner.publish_event", mock_publish_event)
    mocker.patch("app.services.action_runner.config_manager", mock_config_manager_er_provider)
    mocker.patch("app.actions.handlers.AsyncERClient", mock_erclient_class)

    response = await execute_action(
        integration_id=str(er_integration_v2_provider.id),
        action_id="auth",
        config_overrides={
            "authentication_type": "token",
            "token": ""
        }
    )

    assert mock_config_manager_er_provider.get_integration_details.called
    assert not mock_erclient_class.return_value.get_me.called
    assert response.get("valid_credentials") == False
    assert "error" in response


@pytest.mark.asyncio
async def test_execute_auth_action_with_empty_user(
        mocker, mock_gundi_client_v2, mock_erclient_class,
        er_integration_v2_provider, mock_publish_event, mock_config_manager_er_provider
):
    mocker.patch("app.services.action_runner._portal", mock_gundi_client_v2)
    mocker.patch("app.services.activity_logger.publish_event", mock_publish_event)
    mocker.patch("app.services.action_runner.publish_event", mock_publish_event)
    mocker.patch("app.services.action_runner.config_manager", mock_config_manager_er_provider)
    mocker.patch("app.actions.handlers.AsyncERClient", mock_erclient_class)

    response = await execute_action(
        integration_id=str(er_integration_v2_provider.id),
        action_id="auth",
        config_overrides={
            "authentication_type": "username_password",
            "username": "",
            "password": "password"
        }
    )

    assert mock_config_manager_er_provider.get_integration_details.called
    assert not mock_erclient_class.return_value.get_me.called
    assert response.get("valid_credentials") == False
    assert "error" in response


@pytest.mark.asyncio
async def test_execute_show_permissions_action_with_default_config(
        mocker, mock_gundi_client_v2, mock_erclient_class, er_integration_v2_provider,
        mock_publish_event, mock_config_manager_er_destination, expected_permissions_result_with_default_config
):
    mocker.patch("app.services.action_runner._portal", mock_gundi_client_v2)
    mocker.patch("app.services.activity_logger.publish_event", mock_publish_event)
    mocker.patch("app.services.action_runner.publish_event", mock_publish_event)
    mocker.patch("app.services.action_runner.config_manager", mock_config_manager_er_destination)
    mocker.patch("app.actions.handlers.AsyncERClient", mock_erclient_class)

    response = await execute_action(
        integration_id=str(er_integration_v2_provider.id),
        action_id="show_permissions"
    )

    assert mock_config_manager_er_destination.get_integration_details.called
    mock_erclient = mock_erclient_class.return_value
    assert mock_erclient.get_me.called
    assert mock_erclient.get_event_types.called
    assert mock_erclient.get_subjectgroups.called
    assert "ui" in response
    ui_settings = response.get("ui", {})
    assert ui_settings.get("widget") == "DynamicJSONCard"
    permissions = response.get("data", {})
    assert permissions == expected_permissions_result_with_default_config

