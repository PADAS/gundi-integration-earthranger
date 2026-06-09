import json

import pytest
from erclient import ERClientPermissionDenied
from gundi_core.events import LogLevel

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
    # One post_events per event now (per-event state lookup) — was batched before GUNDI-5386.
    total = len(events_batch_one) + len(events_batch_two)
    assert mock_gundi_sensors_client_class.return_value.post_events.call_count == total
    assert response == {
        "events_extracted": total,
        "events_updated": 0,
        "updates_emitted": 0,
        "events_skipped_unchanged": 0,
    }


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
    """Operator-supplied event_type / event_category slugs are resolved to ER UUIDs
    via get_event_types() and the UUIDs (not the slugs) are sent in the filter blob."""
    import json
    pull_events_data = er_integration_v2_provider.get_action_config("pull_events").data
    # These slugs exist in the get_event_types_response conftest fixture:
    #   wildlife_sighting → id 0ae08721-6b7c-4d5e-aeda-cb3d1a38926f
    #   mapipedia_activity_rep → id cdcf6a31-5d64-4a5c-8bf9-bf7d146363d0
    #   category "monitoring" → id 6b359461-aa53-4116-bf2c-04cc580de4ef
    pull_events_data["event_types"] = ["wildlife_sighting", "mapipedia_activity_rep"]
    pull_events_data["event_categories"] = ["monitoring"]

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
    # The filter carries the resolved UUIDs from the fixture, NOT the operator's
    # slugs — ER's events endpoint filters by event_type__id__in (UUIDs), so
    # passing raw slugs would have ER silently drop the filter and return
    # everything matching only the date window (the bug this test now guards).
    assert set(er_filter["event_type"]) == {
        "0ae08721-6b7c-4d5e-aeda-cb3d1a38926f",     # wildlife_sighting
        "cdcf6a31-5d64-4a5c-8bf9-bf7d146363d0",     # mapipedia_activity_rep
    }
    assert er_filter["event_category"] == ["6b359461-aa53-4116-bf2c-04cc580de4ef"]  # monitoring


@pytest.mark.parametrize(
    "filter_date_field, expected_er_key",
    [
        (None, "update_date"),                # default — no config override
        ("updated_at", "update_date"),
        ("event_time", "date_range"),
        ("created_at", "create_date"),
    ],
)
@pytest.mark.asyncio
async def test_pull_events_date_field_maps_to_er_filter_key(
        mocker, filter_date_field, expected_er_key,
        mock_gundi_client_v2, mock_state_manager, mock_erclient_class,
        mock_get_gundi_api_key, mock_gundi_sensors_client_class, er_integration_v2_provider,
        mock_publish_event, mock_gundi_client_v2_class, mock_config_manager_er_provider
):
    """The configured filter_date_field selects which ER filter key the date window goes into."""
    import json
    pull_events_data = er_integration_v2_provider.get_action_config("pull_events").data
    if filter_date_field is not None:
        pull_events_data["filter_date_field"] = filter_date_field

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
    other_keys = {"date_range", "create_date", "update_date"} - {expected_er_key}
    assert expected_er_key in er_filter
    assert "lower" in er_filter[expected_er_key]
    for k in other_keys:
        assert k not in er_filter


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


# ---------------------------------------------------------------------------
# GUNDI-5386: pull_events new-vs-update branching
# ---------------------------------------------------------------------------

from app.actions.handlers import _emit_event_updates, _extract_object_id_from_post_events_response


def test_extract_object_id_from_post_events_response():
    """Pulls object_id out of the typical list-wrapped response, and degrades cleanly."""
    assert _extract_object_id_from_post_events_response(
        [{"object_id": "abc-123", "created_at": "..."}]
    ) == "abc-123"
    assert _extract_object_id_from_post_events_response(
        {"object_id": "single-dict-fallback"}
    ) == "single-dict-fallback"
    assert _extract_object_id_from_post_events_response([]) is None
    assert _extract_object_id_from_post_events_response(None) is None
    assert _extract_object_id_from_post_events_response("not-json") is None


@pytest.mark.asyncio
async def test_emit_event_updates_for_new_note(mocker):
    """A single new note in the ER event payload → one update_event_in_gundi call."""
    mock_update = mocker.patch("app.actions.handlers.update_event_in_gundi")
    mock_update.return_value = async_return({})
    state_record = {
        "gundi_object_id": "gundi-obj-1",
        "updated_at": "2026-06-01T00:00:00Z",
        "state": "active",
        "priority": 100,
        "title": "Snare check",
        "seen_note_ids": ["note-a"],
    }
    er_event = {
        "id": "er-uuid-1",
        "updated_at": "2026-06-02T00:00:00Z",
        "state": "active",
        "priority": 100,
        "title": "Snare check",
        "notes": [
            {"id": "note-a", "text": "old"},
            {"id": "note-b", "text": "fresh observation"},
        ],
    }
    emitted, new_seen = await _emit_event_updates(
        er_event=er_event, state_record=state_record, integration_id="int-1"
    )
    assert emitted == 1
    assert mock_update.call_count == 1
    call_kwargs = mock_update.call_args.kwargs
    assert call_kwargs["gundi_object_id"] == "gundi-obj-1"
    assert call_kwargs["changes"] == {"notes": [{"id": "note-b", "text": "fresh observation"}]}
    assert "note-a" in new_seen and "note-b" in new_seen


@pytest.mark.asyncio
async def test_emit_event_updates_for_field_changes(mocker):
    """Status, priority, and title changes each emit a separate update_event."""
    mock_update = mocker.patch("app.actions.handlers.update_event_in_gundi")
    mock_update.return_value = async_return({})
    state_record = {
        "gundi_object_id": "gundi-obj-2",
        "updated_at": "2026-06-01T00:00:00Z",
        "state": "new",
        "priority": 100,
        "title": "Old title",
        "seen_note_ids": [],
    }
    er_event = {
        "id": "er-uuid-2",
        "updated_at": "2026-06-02T00:00:00Z",
        "state": "active",       # changed
        "priority": 200,         # changed
        "title": "New title",    # changed
        "notes": [],
    }
    emitted, _ = await _emit_event_updates(
        er_event=er_event, state_record=state_record, integration_id="int-2"
    )
    assert emitted == 3
    # Each change goes into the PATCH body under the cdip-side field name (status, not state).
    sent_changes = [c.kwargs["changes"] for c in mock_update.call_args_list]
    assert {"status": "active"} in sent_changes
    assert {"priority": 200} in sent_changes
    assert {"title": "New title"} in sent_changes


@pytest.mark.asyncio
async def test_emit_event_updates_skips_when_nothing_changed(mocker):
    """No new notes and no field diffs → zero update_event_in_gundi calls."""
    mock_update = mocker.patch("app.actions.handlers.update_event_in_gundi")
    mock_update.return_value = async_return({})
    state_record = {
        "gundi_object_id": "gundi-obj-3",
        "updated_at": "2026-06-01T00:00:00Z",
        "state": "active",
        "priority": 100,
        "title": "Snare check",
        "seen_note_ids": ["note-x"],
    }
    er_event = {
        "id": "er-uuid-3",
        "updated_at": "2026-06-02T00:00:00Z",
        "state": "active",
        "priority": 100,
        "title": "Snare check",
        "notes": [{"id": "note-x", "text": "still here"}],
    }
    emitted, _ = await _emit_event_updates(
        er_event=er_event, state_record=state_record, integration_id="int-3"
    )
    assert emitted == 0
    mock_update.assert_not_called()


@pytest.mark.asyncio
async def test_pull_events_skips_event_when_updated_at_unchanged(
        mocker, mock_gundi_client_v2, mock_erclient_class,
        mock_get_gundi_api_key, mock_gundi_sensors_client_class, er_integration_v2_provider,
        mock_publish_event, mock_gundi_client_v2_class, mock_config_manager_er_provider,
        events_batch_one
):
    """If a previously-seen ER event reappears with the same updated_at, we no-op."""
    # State manager that returns a per-event record whose updated_at matches the first event.
    first_event = events_batch_one[0]
    mock_state_manager = mocker.MagicMock()

    async def fake_get_state(integration_id, action_id, source_id="no-source"):
        if source_id == first_event["id"]:
            return {
                "gundi_object_id": "gundi-obj-existing",
                "updated_at": first_event["updated_at"],
                "state": first_event.get("state"),
                "priority": first_event.get("priority"),
                "title": first_event.get("title"),
                "seen_note_ids": [],
            }
        return {"last_execution": "2023-11-17T11:20:00+0200"}

    mock_state_manager.get_state.side_effect = fake_get_state
    mock_state_manager.set_state.return_value = async_return(None)
    # Single-event batch keeps the test focused.
    from app.actions.tests.conftest import AsyncIterator
    mock_erclient_class.return_value.get_events.return_value = AsyncIterator([[first_event]])

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
        action_id="pull_events",
    )

    mock_gundi_sensors_client_class.return_value.post_events.assert_not_called()
    assert response["events_skipped_unchanged"] == 1
    assert response["events_extracted"] == 0


# ---------------------------------------------------------------------------
# Title fallback: use EventType.display when an ER event has no title.
# ---------------------------------------------------------------------------

from app.actions.handlers import (
    EventTypeMaps,
    _fetch_event_type_maps,
    _resolve_slugs,
    transform_events_to_gundi_schema,
)


def test_transform_events_uses_event_title_when_present():
    """An explicit title on the ER event wins over the display map."""
    transformed = transform_events_to_gundi_schema(
        events=[{"id": "x", "event_type": "poacher_sighting_rep", "title": "Snare check"}],
        event_type_display_by_slug={"poacher_sighting_rep": "Poacher Sighting Report"},
    )
    assert transformed[0]["title"] == "Snare check"


def test_transform_events_falls_back_to_display_when_title_missing():
    """No title on the event → use the EventType display name from the map."""
    transformed = transform_events_to_gundi_schema(
        events=[{"id": "x", "event_type": "poacher_sighting_rep"}],
        event_type_display_by_slug={"poacher_sighting_rep": "Poacher Sighting Report"},
    )
    assert transformed[0]["title"] == "Poacher Sighting Report"


def test_transform_events_falls_through_to_slug_when_display_unmapped():
    """No title and the slug isn't in the display map → keep the prior slug fallback."""
    transformed = transform_events_to_gundi_schema(
        events=[{"id": "x", "event_type": "some_unmapped_type"}],
        event_type_display_by_slug={"other": "Other Display"},
    )
    assert transformed[0]["title"] == "some_unmapped_type"


def test_transform_events_omits_title_when_event_type_also_missing():
    """No title, no event_type → no title set (the prior behavior)."""
    transformed = transform_events_to_gundi_schema(
        events=[{"id": "x"}],
        event_type_display_by_slug={"poacher_sighting_rep": "Poacher Sighting Report"},
    )
    assert "title" not in transformed[0]


def test_transform_events_no_display_map_arg_matches_prior_behavior():
    """Calling without the new kwarg preserves the original slug-only fallback."""
    transformed = transform_events_to_gundi_schema(
        events=[{"id": "x", "event_type": "poacher_sighting_rep"}],
    )
    assert transformed[0]["title"] == "poacher_sighting_rep"


@pytest.mark.asyncio
async def test_fetch_event_type_maps_merges_v1_and_v2(mocker):
    """ER's v1 and v2 event-type endpoints each return only their own version's
    types. The helper queries both and merges, with v1 contributing category
    UUIDs (nested object) and v2 contributing slug→id mappings for v2-only types."""
    from erclient import VERSION_1_0, VERSION_2_0
    er_client = mocker.MagicMock()

    async def fake_get_event_types(version=VERSION_1_0, **kwargs):
        if version == VERSION_1_0:
            return [
                {
                    "value": "wildlife_sighting_rep",
                    "display": "Wildlife Sighting Report",
                    "id": "v1-type-uuid",
                    "category": {"value": "wildlife", "id": "cat-uuid-w", "is_active": True},
                },
            ]
        if version == VERSION_2_0:
            return [
                # v2 carries category as a bare slug, not a nested dict.
                {
                    "value": "coyote_carcass",
                    "display": "Coyote Carcass",
                    "id": "v2-type-uuid",
                    "category": "wildlife",
                },
            ]
        return []

    er_client.get_event_types = fake_get_event_types
    maps = await _fetch_event_type_maps(er_client)
    # Both versions contribute to display + id maps.
    assert maps.display_by_slug == {
        "wildlife_sighting_rep": "Wildlife Sighting Report",
        "coyote_carcass": "Coyote Carcass",
    }
    assert maps.id_by_slug == {
        "wildlife_sighting_rep": "v1-type-uuid",
        "coyote_carcass": "v2-type-uuid",
    }
    # v1's nested category dict populates category_id_by_slug; the v2 fetch
    # didn't need to fall back to get_event_categories.
    assert maps.category_id_by_slug == {"wildlife": "cat-uuid-w"}


@pytest.mark.asyncio
async def test_fetch_event_type_maps_falls_back_to_categories_endpoint(mocker):
    """If only v2 event types exist (no nested category UUIDs), the helper hits
    the /event_categories endpoint to populate category_id_by_slug."""
    from erclient import VERSION_1_0, VERSION_2_0
    er_client = mocker.MagicMock()

    async def fake_get_event_types(version=VERSION_1_0, **kwargs):
        if version == VERSION_1_0:
            return []  # ER instance has no v1 types
        return [
            {
                "value": "coyote_carcass",
                "display": "Coyote Carcass",
                "id": "v2-type-uuid",
                "category": "wildlife",
            },
        ]

    async def fake_get_event_categories(**kwargs):
        return [
            {"value": "wildlife", "id": "cat-uuid-w", "display": "Wildlife", "is_active": True},
            {"value": "monitoring", "id": "cat-uuid-m", "display": "Monitoring", "is_active": True},
        ]

    er_client.get_event_types = fake_get_event_types
    er_client.get_event_categories = fake_get_event_categories

    maps = await _fetch_event_type_maps(er_client)
    assert maps.id_by_slug == {"coyote_carcass": "v2-type-uuid"}
    # category_id_by_slug was empty after v1+v2 → categories endpoint backfilled it.
    assert maps.category_id_by_slug == {
        "wildlife": "cat-uuid-w",
        "monitoring": "cat-uuid-m",
    }


@pytest.mark.asyncio
async def test_fetch_event_type_maps_v1_succeeds_v2_fails(mocker):
    """One endpoint failing doesn't break the other."""
    from erclient import VERSION_1_0, VERSION_2_0
    er_client = mocker.MagicMock()

    async def fake_get_event_types(version=VERSION_1_0, **kwargs):
        if version == VERSION_1_0:
            return [
                {
                    "value": "wildlife_sighting_rep",
                    "display": "Wildlife Sighting Report",
                    "id": "v1-type-uuid",
                    "category": {"value": "wildlife", "id": "cat-uuid-w"},
                },
            ]
        raise ERClientPermissionDenied(
            "ER Forbidden ON GET https://gundi-er.pamdas.org/api/v2.0/activity/eventtypes.",
            status_code=403,
            response_body="{}",
        )

    er_client.get_event_types = fake_get_event_types
    maps = await _fetch_event_type_maps(er_client)
    assert maps.id_by_slug == {"wildlife_sighting_rep": "v1-type-uuid"}
    assert maps.category_id_by_slug == {"wildlife": "cat-uuid-w"}


@pytest.mark.asyncio
async def test_fetch_event_type_maps_degrades_to_empty_when_everything_fails(mocker):
    """v1, v2, and categories all 403 → all maps empty (callers will skip pull)."""
    er_client = mocker.MagicMock()

    async def boom_event_types(**kwargs):
        raise ERClientPermissionDenied(
            "ER Forbidden", status_code=403, response_body="{}"
        )

    async def boom_categories(**kwargs):
        raise ERClientPermissionDenied(
            "ER Forbidden", status_code=403, response_body="{}"
        )

    er_client.get_event_types = boom_event_types
    er_client.get_event_categories = boom_categories

    maps = await _fetch_event_type_maps(er_client)
    assert isinstance(maps, EventTypeMaps)
    assert maps.display_by_slug == {}
    assert maps.id_by_slug == {}
    assert maps.category_id_by_slug == {}


def test_resolve_slugs_splits_known_from_unknown():
    """Known slugs map to their IDs; unknowns surface separately for the WARN log."""
    resolved, unresolved = _resolve_slugs(
        ["a", "typo", "b"], {"a": "id-a", "b": "id-b", "c": "id-c"}
    )
    assert resolved == ["id-a", "id-b"]
    assert unresolved == ["typo"]


def test_resolve_slugs_with_empty_inputs():
    """Both axes use this helper; empty config + empty map should noop cleanly."""
    assert _resolve_slugs([], {"a": "id"}) == ([], [])
    assert _resolve_slugs(["a"], {}) == ([], ["a"])


@pytest.mark.asyncio
async def test_pull_events_falls_back_to_slug_when_event_types_unavailable(
        mocker, mock_gundi_client_v2, mock_state_manager, mock_get_gundi_api_key,
        mock_gundi_sensors_client_class, er_integration_v2_provider,
        mock_publish_event, mock_gundi_client_v2_class, mock_config_manager_er_provider,
        mock_erclient_class,
):
    """End-to-end: get_event_types raises → titleless events still flow through to
    Gundi with the event_type slug as their title."""
    # The ER client raises on get_event_types but returns one titleless event
    # from get_events. The handler should swallow the first error, treat the
    # display map as empty, and fall back to the slug for the title.
    async def boom():
        raise ERClientPermissionDenied(
            "ER Forbidden ON GET https://gundi-er.pamdas.org/api/v1.0/activity/events/eventtypes.",
            status_code=403,
            response_body="{}",
        )

    mock_erclient_class.return_value.get_event_types = boom

    from app.actions.tests.conftest import AsyncIterator
    titleless_event = {
        "id": "untitled-event-uuid",
        "event_type": "poacher_sighting_rep",
        # Note: no 'title' key set.
        "updated_at": "2026-06-09T00:00:00Z",
        "created_at": "2026-06-09T00:00:00Z",
    }
    mock_erclient_class.return_value.get_events.return_value = AsyncIterator([[titleless_event]])

    mocker.patch("app.services.action_runner._portal", mock_gundi_client_v2)
    mocker.patch("app.services.activity_logger.publish_event", mock_publish_event)
    mocker.patch("app.services.action_runner.publish_event", mock_publish_event)
    mocker.patch("app.services.action_runner.config_manager", mock_config_manager_er_provider)
    mocker.patch("app.actions.handlers.state_manager", mock_state_manager)
    mocker.patch("app.actions.handlers.AsyncERClient", mock_erclient_class)
    mocker.patch("app.services.gundi.GundiClient", mock_gundi_client_v2_class)
    mocker.patch("app.services.gundi.GundiDataSenderClient", mock_gundi_sensors_client_class)
    mocker.patch("app.services.gundi._get_gundi_api_key", mock_get_gundi_api_key)

    await execute_action(
        integration_id=str(er_integration_v2_provider.id),
        action_id="pull_events",
    )

    # The event was forwarded once with the slug as the title.
    post_events_calls = mock_gundi_sensors_client_class.return_value.post_events.call_args_list
    assert len(post_events_calls) == 1
    posted = post_events_calls[0].kwargs["data"]
    assert posted[0]["title"] == "poacher_sighting_rep"


@pytest.mark.asyncio
async def test_pull_events_skips_when_all_event_type_slugs_unresolvable(
        mocker, mock_gundi_client_v2, mock_state_manager, mock_erclient_class,
        mock_get_gundi_api_key, mock_gundi_sensors_client_class, er_integration_v2_provider,
        mock_publish_event, mock_gundi_client_v2_class, mock_config_manager_er_provider,
):
    """If every configured event_type slug fails to resolve, skip the pull and
    log ERROR — rather than silently fetching everything because ER drops
    invalid filter blobs."""
    pull_events_data = er_integration_v2_provider.get_action_config("pull_events").data
    pull_events_data["event_types"] = ["typo_one", "typo_two"]
    pull_events_data["event_categories"] = []

    mock_log = mocker.patch("app.actions.handlers.log_action_activity")
    mock_log.return_value = async_return(None)

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
        action_id="pull_events",
    )

    mock_erclient_class.return_value.get_events.assert_not_called()
    mock_state_manager.set_state.assert_not_called()  # watermark NOT advanced
    assert response["skipped_reason"] == "no_resolvable_event_types"
    # An ERROR-level activity log named the typoed slugs so ops can fix it.
    error_log = next(
        c for c in mock_log.call_args_list
        if c.kwargs.get("level") == LogLevel.ERROR
    )
    assert error_log.kwargs["data"]["configured_event_types"] == ["typo_one", "typo_two"]


@pytest.mark.asyncio
async def test_pull_events_warns_about_partially_unresolvable_slugs(
        mocker, mock_gundi_client_v2, mock_state_manager, mock_erclient_class,
        mock_get_gundi_api_key, mock_gundi_sensors_client_class, er_integration_v2_provider,
        mock_publish_event, mock_gundi_client_v2_class, mock_config_manager_er_provider,
):
    """A mix of known-good and typoed slugs logs a WARNING and proceeds with
    the known-good ones (better than failing the whole pull)."""
    pull_events_data = er_integration_v2_provider.get_action_config("pull_events").data
    pull_events_data["event_types"] = ["wildlife_sighting", "typo_slug"]
    pull_events_data["event_categories"] = []

    mock_log = mocker.patch("app.actions.handlers.log_action_activity")
    mock_log.return_value = async_return(None)

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

    # We DID call get_events — with the one resolvable slug's UUID in the filter.
    er_filter = json.loads(mock_erclient_class.return_value.get_events.call_args.kwargs["filter"])
    assert er_filter["event_type"] == ["0ae08721-6b7c-4d5e-aeda-cb3d1a38926f"]
    # And we surfaced the typoed slug via a WARNING activity log.
    warning_log = next(
        c for c in mock_log.call_args_list
        if c.kwargs.get("level") == LogLevel.WARNING
    )
    assert warning_log.kwargs["data"]["unresolved_event_types"] == ["typo_slug"]
