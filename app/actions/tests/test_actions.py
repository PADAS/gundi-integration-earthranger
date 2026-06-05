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
    assert response == {
        "observations_extracted": len(observations_batch_one) + len(observations_batch_two),
        "filter_active": False,
        "sources_resolved": None,
    }


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
    # New copy-paste fields are asserted in test_show_permissions_surfaces_raw_slugs_and_uuids.
    for k in ("Event Type Slugs", "Event Category Slugs", "Subject Group UUIDs"):
        assert k in permissions
        permissions.pop(k)
    assert permissions == expected_permissions_result_with_default_config


@pytest.mark.asyncio
async def test_execute_show_permissions_action_with_include_subjects_from_subgroups_true(
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
        action_id="show_permissions",
        config_overrides={
            "include_subjects_from_subgroups_in_parent": True
        }
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
    # New copy-paste fields are asserted in test_show_permissions_surfaces_raw_slugs_and_uuids.
    for k in ("Event Type Slugs", "Event Category Slugs", "Subject Group UUIDs"):
        assert k in permissions
        permissions.pop(k)
    assert permissions == expected_permissions_result_with_default_config


@pytest.mark.asyncio
async def test_execute_show_permissions_action_with_include_subjects_from_subgroups_false(
        mocker, mock_gundi_client_v2, mock_erclient_class, er_integration_v2_provider,
        mock_publish_event, mock_config_manager_er_destination,
        expected_permissions_result_with_include_subjects_from_subgroups_false
):
    mocker.patch("app.services.action_runner._portal", mock_gundi_client_v2)
    mocker.patch("app.services.activity_logger.publish_event", mock_publish_event)
    mocker.patch("app.services.action_runner.publish_event", mock_publish_event)
    mocker.patch("app.services.action_runner.config_manager", mock_config_manager_er_destination)
    mocker.patch("app.actions.handlers.AsyncERClient", mock_erclient_class)

    response = await execute_action(
        integration_id=str(er_integration_v2_provider.id),
        action_id="show_permissions",
        config_overrides={
            "include_subjects_from_subgroups_in_parent": False
        }
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
    for k in ("Event Type Slugs", "Event Category Slugs", "Subject Group UUIDs"):
        assert k in permissions
        permissions.pop(k)
    assert permissions == expected_permissions_result_with_include_subjects_from_subgroups_false


@pytest.mark.asyncio
async def test_execute_show_permissions_action_with_bad_token(
        mocker, mock_gundi_client_v2, mock_erclient_class_with_auth_401, er_integration_v2_provider,
        mock_publish_event, mock_config_manager_er_destination,

):
    mocker.patch("app.services.action_runner._portal", mock_gundi_client_v2)
    mocker.patch("app.services.activity_logger.publish_event", mock_publish_event)
    mocker.patch("app.services.action_runner.publish_event", mock_publish_event)
    mocker.patch("app.services.action_runner.config_manager", mock_config_manager_er_destination)
    mocker.patch("app.actions.handlers.AsyncERClient", mock_erclient_class_with_auth_401)

    response = await execute_action(
        integration_id=str(er_integration_v2_provider.id),
        action_id="show_permissions"
    )

    assert mock_config_manager_er_destination.get_integration_details.called
    mock_erclient = mock_erclient_class_with_auth_401.return_value
    assert mock_erclient.get_me.called
    response_data = response.get("data")
    user_details = response_data.get("User Details", {})
    assert user_details.get("error") == "Invalid credentials. Please provide a valid credentials in the authentication config."


@pytest.mark.asyncio
async def test_execute_show_permissions_action_with_bad_password(
        mocker, mock_gundi_client_v2, mock_erclient_class_with_auth_400, er_integration_v2_provider,
        mock_publish_event, mock_config_manager_er_destination,

):
    mocker.patch("app.services.action_runner._portal", mock_gundi_client_v2)
    mocker.patch("app.services.activity_logger.publish_event", mock_publish_event)
    mocker.patch("app.services.action_runner.publish_event", mock_publish_event)
    mocker.patch("app.services.action_runner.config_manager", mock_config_manager_er_destination)
    mocker.patch("app.actions.handlers.AsyncERClient", mock_erclient_class_with_auth_400)

    response = await execute_action(
        integration_id=str(er_integration_v2_provider.id),
        action_id="show_permissions"
    )

    assert mock_config_manager_er_destination.get_integration_details.called
    mock_erclient = mock_erclient_class_with_auth_400.return_value
    assert mock_erclient.get_me.called
    response_data = response.get("data")
    user_details = response_data.get("User Details", {})
    assert user_details.get("error") == "ER status 400: Invalid credentials given."


@pytest.mark.asyncio
async def test_execute_show_permissions_action_with_403_on_subjectgroups(
        mocker, mock_gundi_client_v2, mock_erclient_class_with_403_on_subjectgroups, er_integration_v2_provider,
        mock_publish_event, mock_config_manager_er_destination, mock_er_403_on_subjectgroups_exception

):
    mocker.patch("app.services.action_runner._portal", mock_gundi_client_v2)
    mocker.patch("app.services.activity_logger.publish_event", mock_publish_event)
    mocker.patch("app.services.action_runner.publish_event", mock_publish_event)
    mocker.patch("app.services.action_runner.config_manager", mock_config_manager_er_destination)
    mocker.patch("app.actions.handlers.AsyncERClient", mock_erclient_class_with_403_on_subjectgroups)

    response = await execute_action(
        integration_id=str(er_integration_v2_provider.id),
        action_id="show_permissions"
    )

    assert mock_config_manager_er_destination.get_integration_details.called
    mock_erclient = mock_erclient_class_with_403_on_subjectgroups.return_value
    assert mock_erclient.get_me.called
    response_data = response.get("data")
    user_details = response_data.get("Subject Groups", {})
    exc = mock_er_403_on_subjectgroups_exception
    assert user_details.get("error") == f"Error retrieving subject groups: {type(exc).__name__}:{exc}"


# ---------------------------------------------------------------------------
# Filtering: pull_events event_types / event_categories
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_pull_events_event_type_and_category_filter_passed_to_er(
        mocker, mock_gundi_client_v2, mock_state_manager, mock_erclient_class,
        mock_get_gundi_api_key, mock_gundi_sensors_client_class, er_integration_v2_provider,
        mock_publish_event, mock_gundi_client_v2_class, mock_config_manager_er_provider
):
    """The configured event_types / event_categories are injected into the ER filter dict."""
    import json
    pull_events_data = er_integration_v2_provider.get_action_config("pull_events").data
    pull_events_data["event_types"] = ["poacher_sighting_rep", "wildlife_sighting_rep"]
    pull_events_data["event_categories"] = ["wildlife"]

    mocker.patch("app.services.activity_logger.publish_event", mock_publish_event)
    mocker.patch("app.services.action_runner.publish_event", mock_publish_event)
    mocker.patch("app.services.action_runner.config_manager", mock_config_manager_er_provider)
    mocker.patch("app.services.action_runner._portal", mock_gundi_client_v2)
    mocker.patch("app.actions.handlers.state_manager", mock_state_manager)
    mocker.patch("app.actions.handlers.AsyncERClient", mock_erclient_class)
    mocker.patch("app.services.gundi.GundiClient", mock_gundi_client_v2_class)
    mocker.patch("app.services.gundi.GundiDataSenderClient", mock_gundi_sensors_client_class)
    mocker.patch("app.services.gundi._get_gundi_api_key", mock_get_gundi_api_key)

    await execute_action(
        integration_id=str(er_integration_v2_provider.id),
        action_id="pull_events",
    )

    er_filter = json.loads(mock_erclient_class.return_value.get_events.call_args.kwargs["filter"])
    assert er_filter["event_type"] == ["poacher_sighting_rep", "wildlife_sighting_rep"]
    assert er_filter["event_category"] == ["wildlife"]


# ---------------------------------------------------------------------------
# Filtering: _resolve_source_ids unit tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_resolve_source_ids_walks_subgroups(mocker):
    """A matched parent group includes every descendant subject's sources."""
    from app.actions.handlers import _resolve_source_ids

    parent_id = "parent-uuid"
    er_client = mocker.MagicMock()

    async def fake_get_subjectgroups(flat=False):
        return [{
            "id": parent_id,
            "name": "Parent",
            "subjects": [{"id": "subj-direct"}],
            "subgroups": [{
                "id": "child-uuid",
                "name": "Child",
                "subjects": [{"id": "subj-nested"}],
                "subgroups": [],
            }],
        }]

    async def fake_get_source_assignments(subject_ids=None, source_ids=None):
        assert set(subject_ids) == {"subj-direct", "subj-nested"}
        return [
            {"subject": "subj-direct", "source": "src-1"},
            {"subject": "subj-nested", "source": "src-2"},
        ]

    er_client.get_subjectgroups.side_effect = fake_get_subjectgroups
    er_client.get_source_assignments.side_effect = fake_get_source_assignments

    sources = await _resolve_source_ids(er_client, group_ids=[parent_id])

    assert sources == {"src-1", "src-2"}


@pytest.mark.asyncio
async def test_resolve_source_ids_dedups_across_overlapping_groups(mocker):
    """A subject in two configured groups only triggers one assignment lookup."""
    from app.actions.handlers import _resolve_source_ids

    er_client = mocker.MagicMock()

    async def fake_get_subjectgroups(flat=False):
        return [
            {
                "id": "group-a",
                "subjects": [{"id": "shared-subj"}, {"id": "subj-a"}],
                "subgroups": [],
            },
            {
                "id": "group-b",
                "subjects": [{"id": "shared-subj"}, {"id": "subj-b"}],
                "subgroups": [],
            },
        ]

    async def fake_get_source_assignments(subject_ids=None, source_ids=None):
        # Dedup before lookup: shared-subj appears once.
        assert sorted(subject_ids) == ["shared-subj", "subj-a", "subj-b"]
        return [{"subject": s, "source": f"src-{s}"} for s in subject_ids]

    er_client.get_subjectgroups.side_effect = fake_get_subjectgroups
    er_client.get_source_assignments.side_effect = fake_get_source_assignments

    sources = await _resolve_source_ids(er_client, group_ids=["group-a", "group-b"])

    assert sources == {"src-shared-subj", "src-subj-a", "src-subj-b"}
    assert er_client.get_source_assignments.call_count == 1


@pytest.mark.asyncio
async def test_resolve_source_ids_empty_group_ids_short_circuits(mocker):
    """No configured groups → no ER calls at all."""
    from app.actions.handlers import _resolve_source_ids

    er_client = mocker.MagicMock()
    sources = await _resolve_source_ids(er_client, group_ids=[])

    assert sources == set()
    er_client.get_subjectgroups.assert_not_called()
    er_client.get_source_assignments.assert_not_called()


# ---------------------------------------------------------------------------
# Filtering: pull_observations end-to-end
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_pull_observations_filters_by_resolved_source_ids(
        mocker, mock_gundi_client_v2, mock_state_manager, mock_erclient_class,
        mock_get_gundi_api_key, mock_gundi_sensors_client_class, er_integration_v2_provider,
        mock_publish_event, mock_gundi_client_v2_class, mock_config_manager_er_provider
):
    """When subject_group_ids is set, only observations whose source is in the resolved set are forwarded."""
    pull_obs_data = er_integration_v2_provider.get_action_config("pull_observations").data
    pull_obs_data["subject_group_ids"] = ["group-uuid"]

    # Override the er_client mocks to control resolution + observation batch
    async def fake_get_subjectgroups(flat=False):
        return [{
            "id": "group-uuid",
            "subjects": [{"id": "subj-1"}, {"id": "subj-2"}],
            "subgroups": [],
        }]

    async def fake_get_source_assignments(subject_ids=None, source_ids=None):
        return [
            {"subject": "subj-1", "source": "src-keep-1"},
            {"subject": "subj-2", "source": "src-keep-2"},
        ]

    mocker.patch.object(
        mock_erclient_class.return_value, "get_subjectgroups", side_effect=fake_get_subjectgroups
    )
    mocker.patch.object(
        mock_erclient_class.return_value, "get_source_assignments", side_effect=fake_get_source_assignments
    )
    # Mixed observation batch: 2 keepers, 1 reject
    from app.actions.tests.conftest import AsyncIterator
    mock_erclient_class.return_value.get_observations.return_value = AsyncIterator([[
        {"id": "obs-1", "source": "src-keep-1", "recorded_at": "2025-01-01T00:00:00Z"},
        {"id": "obs-2", "source": "src-reject", "recorded_at": "2025-01-01T00:00:01Z"},
        {"id": "obs-3", "source": "src-keep-2", "recorded_at": "2025-01-01T00:00:02Z"},
    ]])

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
        action_id="pull_observations",
    )

    assert response == {
        "observations_extracted": 2,
        "filter_active": True,
        "sources_resolved": 2,
    }
    forwarded = mock_gundi_sensors_client_class.return_value.post_observations.call_args.kwargs["data"]
    # transform_observations_to_gundi_schema prefixes the source with "er-src-"
    forwarded_sources = {o["source"] for o in forwarded}
    assert forwarded_sources == {"er-src-src-keep-1", "er-src-src-keep-2"}


@pytest.mark.asyncio
async def test_pull_observations_logs_error_and_skips_state_when_groups_resolve_to_zero(
        mocker, mock_gundi_client_v2, mock_state_manager, mock_erclient_class,
        mock_get_gundi_api_key, mock_gundi_sensors_client_class, er_integration_v2_provider,
        mock_publish_event, mock_gundi_client_v2_class, mock_config_manager_er_provider
):
    """When configured groups resolve to zero sources, log ERROR, do NOT advance state, return zero."""
    pull_obs_data = er_integration_v2_provider.get_action_config("pull_observations").data
    pull_obs_data["subject_group_ids"] = ["empty-group-uuid"]

    async def fake_get_subjectgroups(flat=False):
        # Group exists but has no subjects.
        return [{"id": "empty-group-uuid", "subjects": [], "subgroups": []}]

    mocker.patch.object(
        mock_erclient_class.return_value, "get_subjectgroups", side_effect=fake_get_subjectgroups
    )
    mock_log_activity = mocker.patch("app.actions.handlers.log_action_activity")
    mock_log_activity.return_value = async_return(None)

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
        action_id="pull_observations",
    )

    assert response == {
        "observations_extracted": 0,
        "filter_active": True,
        "sources_resolved": 0,
    }
    mock_state_manager.set_state.assert_not_called()
    mock_log_activity.assert_called_once()
    log_kwargs = mock_log_activity.call_args.kwargs
    from gundi_core.events import LogLevel
    assert log_kwargs["level"] == LogLevel.ERROR
    assert "zero active sources" in log_kwargs["title"]


# ---------------------------------------------------------------------------
# show_permissions: raw slug/UUID fields
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_show_permissions_surfaces_raw_slugs_and_uuids(
        mocker, mock_gundi_client_v2, mock_erclient_class, er_integration_v2_provider,
        mock_publish_event, mock_config_manager_er_destination
):
    """show_permissions output exposes Event Type Slugs, Event Category Slugs, and Subject Group UUIDs."""
    mocker.patch("app.services.action_runner._portal", mock_gundi_client_v2)
    mocker.patch("app.services.activity_logger.publish_event", mock_publish_event)
    mocker.patch("app.services.action_runner.publish_event", mock_publish_event)
    mocker.patch("app.services.action_runner.config_manager", mock_config_manager_er_destination)
    mocker.patch("app.actions.handlers.AsyncERClient", mock_erclient_class)

    response = await execute_action(
        integration_id=str(er_integration_v2_provider.id),
        action_id="show_permissions",
        config_overrides={"include_subjects_from_subgroups_in_parent": True},
    )

    data = response["data"]
    assert isinstance(data.get("Event Type Slugs"), list)
    assert isinstance(data.get("Event Category Slugs"), list)
    assert isinstance(data.get("Subject Group UUIDs"), list)
    # Slugs are sorted strings, not display names.
    assert data["Event Type Slugs"] == sorted(data["Event Type Slugs"])
    assert all(isinstance(uuid, str) for uuid in data["Subject Group UUIDs"])
