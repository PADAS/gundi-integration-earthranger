import asyncio

import httpx
import pytest
from erclient import ERClientException, ERClientBadCredentials, ERClientPermissionDenied
from gundi_core.schemas.v2 import Integration, IntegrationSummary


def async_return(result):
    f = asyncio.Future()
    f.set_result(result)
    return f


@pytest.fixture
def er_integration_v2_provider():
    return Integration.parse_obj(
        {'id': '779ff3ab-5589-4f4c-9e0a-ae8d6c9edff0', 'name': 'Gundi ER', 'base_url': 'https://gundi-er.pamdas.org',
         'enabled': True,
         'type': {'id': '50229e21-a9fe-4caa-862c-8592dfb2479b', 'name': 'EarthRanger', 'value': 'earth_ranger',
                  'description': 'Integration type for Earth Ranger Sites', 'actions': [
                 {'id': '80448d1c-4696-4b32-a59f-f3494fc949ac', 'type': 'auth', 'name': 'Authenticate', 'value': 'auth',
                  'description': 'Authenticate against Earth Ranger',
                  'schema': {'type': 'object', 'required': ['token'], 'properties': {'token': {'type': 'string'}}}},
                 {'id': '4b721b37-f4ca-4f20-b07c-2caadb095ecb', 'type': 'pull', 'name': 'Pull Events',
                  'value': 'pull_events', 'description': 'Extract events from EarthRanger sites',
                  'schema': {'type': 'object', 'title': 'PullObservationsConfig', 'required': ['start_datetime'],
                             'properties': {'start_datetime': {'type': 'string', 'title': 'Start Datetime'}}}},
                 {'id': '75b3040f-ab1f-42e7-b39f-8965c088b154', 'type': 'pull', 'name': 'Pull Observations',
                  'value': 'pull_observations', 'description': 'Extract observations from an EarthRanger Site',
                  'schema': {'type': 'object', 'title': 'PullObservationsConfig', 'required': ['start_datetime'],
                             'properties': {'start_datetime': {'type': 'string', 'title': 'Start Datetime'}}}},
                 {'id': '425a2e2f-ae71-44fb-9314-bc0116638e4f', 'type': 'push', 'name': 'Push Event Attachments',
                  'value': 'push_event_attachments',
                  'description': 'EarthRanger sites support adding attachments to events', 'schema': {}},
                 {'id': '8e101f31-e693-404c-b6ee-20fde6019f16', 'type': 'push', 'name': 'Push Events',
                  'value': 'push_events', 'description': 'EarthRanger sites support sending Events (a.k.a Reports)',
                  'schema': {}}]},
         'owner': {'id': 'a91b400b-482a-4546-8fcb-ee42b01deeb6', 'name': 'Test Org', 'description': ''},
         'configurations': [
             {'id': '5577c323-b961-4277-9047-b1f27fd6a1b7', 'integration': '779ff3ab-5589-4f4c-9e0a-ae8d6c9edff0',
              'action': {'id': '75b3040f-ab1f-42e7-b39f-8965c088b154', 'type': 'pull', 'name': 'Pull Observations',
                         'value': 'pull_observations'},
              'data': {'end_datetime': '2023-11-10T06:00:00-00:00', 'start_datetime': '2023-11-10T05:30:00-00:00',
                       'force_run_since_start': False}},
             {'id': '431af42b-c431-40af-8b57-a349253e15df', 'integration': '779ff3ab-5589-4f4c-9e0a-ae8d6c9edff0',
              'action': {'id': '4b721b37-f4ca-4f20-b07c-2caadb095ecb', 'type': 'pull', 'name': 'Pull Events',
                         'value': 'pull_events'}, 'data': {'start_datetime': '2023-11-16T00:00:00-03:00'}},
             {'id': '30f8878c-4a98-4c95-88eb-79f73c40fb2f', 'integration': '779ff3ab-5589-4f4c-9e0a-ae8d6c9edff0',
              'action': {'id': '80448d1c-4696-4b32-a59f-f3494fc949ac', 'type': 'auth', 'name': 'Authenticate',
                         'value': 'auth'}, 'data': {'token': 'testtoken2a97022f21732461ee103a08fac8a35'}}],
         'additional': {'topic': 'gundi-er-dispatcher-iD9M1ON-topic', 'broker': 'gcp_pubsub'},
         'default_route': {'id': '5abf3845-7c9f-478a-bc0f-b24d87038c4b', 'name': 'Gundi ER Provider - Default Route'},
         'status': 'healthy',
         'status_details': ''
         }
    )


@pytest.fixture
def er_integration_v2_destination():
    return Integration.parse_obj(
        {
            'id': '282e46af-4189-4330-94ed-2090a769af3e',
            'name': 'Gundi Dev Stage',
            'type': {
                'id': '7c890e6d-162f-4f01-8fc8-386d5a56a5e5',
                'name': 'Earth Ranger',
                'value': 'earth_ranger',
                'description': 'Default type for integrations with Earth Ranger',
                'actions': [{
                    'id': '8f66ec6f-8878-44ea-a759-e0c7363dd1cc',
                    'type': 'auth',
                    'name': 'Auth',
                    'value': 'auth',
                    'description': 'Earth Ranger Auth action',
                    'action_schema': {},
                    'ui_schema': {
                        'ui:order': ['authentication_type', 'token', 'username', 'password']
                    }
                }, {
                    'id': '0a9e245a-3f81-4286-9351-94d266e73256',
                    'type': 'pull',
                    'name': 'Pull Events',
                    'value': 'pull_events',
                    'description': 'Earth Ranger Pull Events action',
                    'action_schema': {},
                    'ui_schema': {
                        'ui:order': ['start_datetime', 'end_datetime', 'force_run_since_start']
                    }
                }, {
                    'id': 'f7ef9e13-c0cf-4db5-aa4f-2f306e4a79f1',
                    'type': 'pull',
                    'name': 'Pull Observations',
                    'value': 'pull_observations',
                    'description': 'Earth Ranger Pull Observations action',
                    'action_schema': {},
                    'ui_schema': {
                        'ui:order': ['start_datetime', 'end_datetime', 'force_run_since_start']
                    }
                }, {
                    'id': '7c819fc1-65c2-4ca1-b621-92d86ebcb124',
                    'type': 'push',
                    'name': 'Push Event Attachments',
                    'value': 'push_event_attachments',
                    'description': 'EarthRanger sites support adding attachments to events',
                    'action_schema': {},
                    'ui_schema': {}
                }, {
                    'id': '4c0f2587-bc6d-4793-a5cf-84916e5c6f14',
                    'type': 'push',
                    'name': 'Push Events',
                    'value': 'push_events',
                    'description': 'EarthRanger sites support sending Events (a.k.a Reports)',
                    'action_schema': {},
                    'ui_schema': {}
                }, {
                    'id': '4991c420-7591-497a-90da-91898b59dfd1',
                    'type': 'push',
                    'name': 'Push Observations',
                    'value': 'push_observations',
                    'description': 'EarthRanger sites support sending Observations (a.k.a Positions)',
                    'action_schema': {},
                    'ui_schema': {}
                }, {
                    'id': '8941d56e-5573-4c29-a600-15ead106790b',
                    'type': 'generic',
                    'name': 'Show Permissions',
                    'value': 'show_permissions',
                    'description': 'Earth Ranger Show Permissions action',
                    'action_schema': {},
                    'ui_schema': {}
                }],
                'webhook': None
            },
            'base_url': 'https://gundi-dev.staging.pamdas.org/',
            'enabled': True,
            'owner': {
                'id': '72504564-9145-477e-a3c8-77fd1d553870',
                'name': 'Demo Org',
                'description': 'Org with Connections we can use in demonstrations'
            },
            'configurations': [{
                'id': '3d069266-9042-4bb4-859b-3492ab0e1e38',
                'integration': '282e46af-4189-4330-94ed-2090a769af3e',
                'action': {
                    'id': '8f66ec6f-8878-44ea-a759-e0c7363dd1cc',
                    'type': 'auth',
                    'name': 'Auth',
                    'value': 'auth'
                },
                'data': {
                    'token': 'testtoken1234',
                    'password': '',
                    'username': '',
                    'authentication_type': 'token'
                }
            }, {
                'id': 'e17e17fa-e132-4d7a-ba95-0977093e4d89',
                'integration': '282e46af-4189-4330-94ed-2090a769af3e',
                'action': {
                    'id': '7c819fc1-65c2-4ca1-b621-92d86ebcb124',
                    'type': 'push',
                    'name': 'Push Event Attachments',
                    'value': 'push_event_attachments'
                },
                'data': {}
            }, {
                'id': 'ec53c541-30e5-4eb9-b72d-9291267115a5',
                'integration': '282e46af-4189-4330-94ed-2090a769af3e',
                'action': {
                    'id': '4c0f2587-bc6d-4793-a5cf-84916e5c6f14',
                    'type': 'push',
                    'name': 'Push Events',
                    'value': 'push_events'
                },
                'data': {}
            }, {
                'id': 'f03a63ed-df84-420b-b1d9-6436c2e290ac',
                'integration': '282e46af-4189-4330-94ed-2090a769af3e',
                'action': {
                    'id': '4991c420-7591-497a-90da-91898b59dfd1',
                    'type': 'push',
                    'name': 'Push Observations',
                    'value': 'push_observations'
                },
                'data': {}
            }, {
                'id': '1bc062b5-30d3-461d-bff2-d65376859535',
                'integration': '282e46af-4189-4330-94ed-2090a769af3e',
                'action': {
                    'id': '8941d56e-5573-4c29-a600-15ead106790b',
                    'type': 'generic',
                    'name': 'Show Permissions',
                    'value': 'show_permissions'
                },
                'data': {}
            }],
            'webhook_configuration': None,
            'default_route': None,
            'additional': {
                'topic': 'gundi-dev-earthran-cesZ3wN-topic',
                'broker': 'gcp_pubsub'
            },
            'status': 'unknown',
            'status_details': ''
        }
    )


@pytest.fixture
def er_integration_v2_with_empty_url():
    return Integration.parse_obj(
        {'id': '779ff3ab-5589-4f4c-9e0a-ae8d6c9edff0', 'name': 'Gundi ER', 'base_url': 'https:///',
         'enabled': True,
         'type': {'id': '50229e21-a9fe-4caa-862c-8592dfb2479b', 'name': 'EarthRanger', 'value': 'earth_ranger',
                  'description': 'Integration type for Earth Ranger Sites', 'actions': [
                 {'id': '80448d1c-4696-4b32-a59f-f3494fc949ac', 'type': 'auth', 'name': 'Authenticate', 'value': 'auth',
                  'description': 'Authenticate against Earth Ranger',
                  'schema': {'type': 'object', 'required': ['token'], 'properties': {'token': {'type': 'string'}}}},
                 {'id': '4b721b37-f4ca-4f20-b07c-2caadb095ecb', 'type': 'pull', 'name': 'Pull Events',
                  'value': 'pull_events', 'description': 'Extract events from EarthRanger sites',
                  'schema': {'type': 'object', 'title': 'PullObservationsConfig', 'required': ['start_datetime'],
                             'properties': {'start_datetime': {'type': 'string', 'title': 'Start Datetime'}}}},
                 {'id': '75b3040f-ab1f-42e7-b39f-8965c088b154', 'type': 'pull', 'name': 'Pull Observations',
                  'value': 'pull_observations', 'description': 'Extract observations from an EarthRanger Site',
                  'schema': {'type': 'object', 'title': 'PullObservationsConfig', 'required': ['start_datetime'],
                             'properties': {'start_datetime': {'type': 'string', 'title': 'Start Datetime'}}}},
                 {'id': '425a2e2f-ae71-44fb-9314-bc0116638e4f', 'type': 'push', 'name': 'Push Event Attachments',
                  'value': 'push_event_attachments',
                  'description': 'EarthRanger sites support adding attachments to events', 'schema': {}},
                 {'id': '8e101f31-e693-404c-b6ee-20fde6019f16', 'type': 'push', 'name': 'Push Events',
                  'value': 'push_events', 'description': 'EarthRanger sites support sending Events (a.k.a Reports)',
                  'schema': {}}]},
         'owner': {'id': 'a91b400b-482a-4546-8fcb-ee42b01deeb6', 'name': 'Test Org', 'description': ''},
         'configurations': [
             {'id': '5577c323-b961-4277-9047-b1f27fd6a1b7', 'integration': '779ff3ab-5589-4f4c-9e0a-ae8d6c9edff0',
              'action': {'id': '75b3040f-ab1f-42e7-b39f-8965c088b154', 'type': 'pull', 'name': 'Pull Observations',
                         'value': 'pull_observations'},
              'data': {'end_datetime': '2023-11-10T06:00:00-00:00', 'start_datetime': '2023-11-10T05:30:00-00:00',
                       'force_run_since_start': False}},
             {'id': '431af42b-c431-40af-8b57-a349253e15df', 'integration': '779ff3ab-5589-4f4c-9e0a-ae8d6c9edff0',
              'action': {'id': '4b721b37-f4ca-4f20-b07c-2caadb095ecb', 'type': 'pull', 'name': 'Pull Events',
                         'value': 'pull_events'}, 'data': {'start_datetime': '2023-11-16T00:00:00-03:00'}},
             {'id': '30f8878c-4a98-4c95-88eb-79f73c40fb2f', 'integration': '779ff3ab-5589-4f4c-9e0a-ae8d6c9edff0',
              'action': {'id': '80448d1c-4696-4b32-a59f-f3494fc949ac', 'type': 'auth', 'name': 'Authenticate',
                         'value': 'auth'}, 'data': {'token': 'testtoken2a97022f21732461ee103a08fac8a35'}}],
         'additional': {'topic': 'gundi-er-dispatcher-iD9M1ON-topic', 'broker': 'gcp_pubsub'},
         'default_route': {'id': '5abf3845-7c9f-478a-bc0f-b24d87038c4b', 'name': 'Gundi ER Provider - Default Route'},
         'status': 'healthy',
         'status_details': ''
         }
    )


@pytest.fixture
def mock_gundi_client_v2(
        mocker,
        er_integration_v2_provider,
):
    mock_client = mocker.MagicMock()
    mock_client.get_integration_details.return_value = async_return(
        er_integration_v2_provider
    )
    mock_client.__aenter__.return_value = mock_client
    return mock_client


@pytest.fixture
def mock_state_manager(mocker):
    mock_state_manager = mocker.MagicMock()
    mock_state_manager.get_state.return_value = async_return(
        {'last_execution': '2023-11-17T11:20:00+0200'}
    )
    mock_state_manager.set_state.return_value = async_return(None)
    return mock_state_manager


@pytest.fixture
def mock_erclient_class(
        mocker,
        auth_headers_response,
        get_me_response,
        get_event_types_response,
        get_subjectgroups_response,
        get_subjectgroups_flat_response,
        get_events_response,
        get_observations_response,
        er_client_close_response
):
    mocked_erclient_class = mocker.MagicMock()
    erclient_mock = mocker.MagicMock()
    erclient_mock.get_me.return_value = async_return(
        get_me_response
    )
    erclient_mock.get_event_types.return_value = async_return(get_event_types_response)

    async def mock_get_subjectgroups(flat=False):
        return get_subjectgroups_flat_response if flat else get_subjectgroups_response

    erclient_mock.get_subjectgroups.side_effect = mock_get_subjectgroups
    erclient_mock.auth_headers.return_value = async_return(
        auth_headers_response
    )
    erclient_mock.get_events.return_value = AsyncIterator(get_events_response)
    erclient_mock.get_observations.return_value = AsyncIterator(get_observations_response)
    erclient_mock.close.return_value = async_return(
        er_client_close_response
    )
    erclient_mock.__aenter__.return_value = erclient_mock
    erclient_mock.__aexit__.return_value = er_client_close_response
    mocked_erclient_class.return_value = erclient_mock
    return mocked_erclient_class


@pytest.fixture
def er_400_invalid_credentials_exception():
    return httpx.HTTPStatusError(
        message="Client error '400 Bad Request' for url 'https://gundi-dev.staging.pamdas.org/oauth2/token'. For more information check: https://httpstatuses.com/400",
        request=httpx.Request("POST", "https://gundi-dev.staging.pamdas.org/oauth2/token"),
        response=httpx.Response(
            status_code=400,
            text='{"error": "invalid_grant", "error_description": "Invalid credentials given."}'
        )
    )


@pytest.fixture
def er_401_exception():
    return ERClientBadCredentials(
        'ER Unauthorized ON GET https://gundi-dev.staging.pamdas.org/api/v1.0/user/me.',
        status_code=401,
        response_body='{"status":{"code":401,"message":"Unauthorized","detail":"Authentication credentials were not provided."}}'
    )


@pytest.fixture
def mock_er_403_on_subjectgroups_exception():
    return ERClientPermissionDenied(
        'ER Forbidden ON GET https://gundi-dev.staging.pamdas.org/api/v1.0/subjectgroups.',
        status_code=403,
        response_body='{"status":{"code":403,"message":"Forbidden","detail":"You do not have permission to perform this action."}}'
    )


@pytest.fixture
def er_500_exception():
    return ERClientException(
        'Failed to GET to ER web service. provider_key: None, service: https://gundi-dev.staging.pamdas.org/api/v1.0, path: user/me,\n\t 500 from ER. Message: duplicate key value violates unique constraint "observations_observation_tenant_source_at_unique"'
    )


@pytest.fixture
def er_generic_exception():
    return ERClientException(
        'Failed to GET to ER web service. provider_key: None, service: https://gundi-dev.staging.pamdas.org/api/v1.0, path: user/me,\n\t Error from ER. Message: Something went wrong'
    )


@pytest.fixture
def er_connect_error():
    return httpx.ConnectError("[Errno -3] Temporary failure in name resolution")


@pytest.fixture
def er_read_timeout_error():
    return httpx.ReadTimeout("Read timeout")


@pytest.fixture
def mock_erclient_class_with_error(
    request,
    mocker,
    er_401_exception,
    er_500_exception,
    er_generic_exception,
    er_connect_error,
    er_read_timeout_error,
    er_client_close_response
):

    if request.param == "er_401_exception":
        er_error = er_401_exception
    elif request.param == "er_500_exception":
        er_error = er_500_exception
    elif request.param == "er_connect_error":
        er_error = er_connect_error
    elif request.param == "er_read_timeout_error":
        er_error = er_read_timeout_error
    else:
        er_error = er_generic_exception
    mocked_erclient_class = mocker.MagicMock()
    erclient_mock = mocker.MagicMock()
    erclient_mock.get_me.side_effect = er_error
    erclient_mock.auth_headers.side_effect = er_error
    erclient_mock.get_events.side_effect = er_error
    erclient_mock.get_observations.side_effect = er_error
    erclient_mock.close.return_value = async_return(
        er_client_close_response
    )
    erclient_mock.__aenter__.return_value = erclient_mock
    erclient_mock.__aexit__.return_value = er_client_close_response
    mocked_erclient_class.return_value = erclient_mock
    return mocked_erclient_class


@pytest.fixture
def mock_erclient_class_with_auth_400(
        mocker,
        auth_headers_response,
        er_400_invalid_credentials_exception,
        er_client_close_response
):
    mocked_erclient_class = mocker.MagicMock()
    erclient_mock = mocker.MagicMock()
    erclient_mock.get_me.side_effect = er_400_invalid_credentials_exception
    erclient_mock.get_event_types.side_effect = er_400_invalid_credentials_exception
    erclient_mock.get_subjectgroups.side_effect = er_400_invalid_credentials_exception
    erclient_mock.auth_headers.side_effect = er_400_invalid_credentials_exception
    erclient_mock.get_events.side_effect = er_400_invalid_credentials_exception
    erclient_mock.get_observations.side_effect = er_400_invalid_credentials_exception
    erclient_mock.close.return_value = async_return(
        er_client_close_response
    )
    erclient_mock.__aenter__.return_value = erclient_mock
    erclient_mock.__aexit__.return_value = er_client_close_response
    mocked_erclient_class.return_value = erclient_mock
    return mocked_erclient_class


@pytest.fixture
def mock_erclient_class_with_auth_401(
        mocker,
        auth_headers_response,
        er_401_exception,
        er_client_close_response
):
    mocked_erclient_class = mocker.MagicMock()
    erclient_mock = mocker.MagicMock()
    erclient_mock.get_me.side_effect = er_401_exception
    erclient_mock.get_event_types.side_effect = er_401_exception
    erclient_mock.get_subjectgroups.side_effect = er_401_exception
    erclient_mock.auth_headers.side_effect = er_401_exception
    erclient_mock.get_events.side_effect = er_401_exception
    erclient_mock.get_observations.side_effect = er_401_exception
    erclient_mock.close.return_value = async_return(
        er_client_close_response
    )
    erclient_mock.__aenter__.return_value = erclient_mock
    erclient_mock.__aexit__.return_value = er_client_close_response
    mocked_erclient_class.return_value = erclient_mock
    return mocked_erclient_class


@pytest.fixture
def mock_erclient_class_with_auth_500(
        mocker,
        auth_headers_response,
        er_500_exception,
        get_events_response,
        get_observations_response,
        er_client_close_response
):
    mocked_erclient_class = mocker.MagicMock()
    erclient_mock = mocker.MagicMock()
    erclient_mock.get_me.side_effect = er_500_exception
    erclient_mock.auth_headers.side_effect = er_500_exception
    erclient_mock.get_events.side_effect = er_500_exception
    erclient_mock.get_observations.side_effect = er_500_exception
    erclient_mock.close.return_value = async_return(
        er_client_close_response
    )
    erclient_mock.__aenter__.return_value = erclient_mock
    erclient_mock.__aexit__.return_value = er_client_close_response
    mocked_erclient_class.return_value = erclient_mock
    return mocked_erclient_class


@pytest.fixture
def mock_erclient_class_with_403_on_subjectgroups(
        mocker,
        auth_headers_response,
        get_me_response,
        get_event_types_response,
        mock_er_403_on_subjectgroups_exception,
        get_events_response,
        get_observations_response,
        er_client_close_response
):
    mocked_erclient_class = mocker.MagicMock()
    erclient_mock = mocker.MagicMock()
    erclient_mock.get_me.return_value = async_return(
        get_me_response
    )
    erclient_mock.get_event_types.return_value = async_return(get_event_types_response)
    erclient_mock.get_subjectgroups.side_effect = mock_er_403_on_subjectgroups_exception
    erclient_mock.auth_headers.return_value = async_return(
        auth_headers_response
    )
    erclient_mock.get_events.return_value = AsyncIterator(get_events_response)
    erclient_mock.get_observations.return_value = AsyncIterator(get_observations_response)
    erclient_mock.close.return_value = async_return(
        er_client_close_response
    )
    erclient_mock.__aenter__.return_value = erclient_mock
    erclient_mock.__aexit__.return_value = er_client_close_response
    mocked_erclient_class.return_value = erclient_mock
    return mocked_erclient_class


@pytest.fixture
def mock_gundi_sensors_client_class(mocker, events_created_response, observations_created_response):
    mock_gundi_sensors_client_class = mocker.MagicMock()
    mock_gundi_sensors_client = mocker.MagicMock()
    mock_gundi_sensors_client.post_events.return_value = async_return(
        events_created_response
    )
    mock_gundi_sensors_client.post_observations.return_value = async_return(
        observations_created_response
    )
    mock_gundi_sensors_client_class.return_value = mock_gundi_sensors_client
    return mock_gundi_sensors_client_class


@pytest.fixture
def events_created_response():
    return [
        {
            "object_id": "abebe106-3c50-446b-9c98-0b9b503fc900",
            "created_at": "2023-11-16T19:59:50.612864Z"
        },
        {
            "object_id": "cdebe106-3c50-446b-9c98-0b9b503fc911",
            "created_at": "2023-11-16T19:59:50.612864Z"
        }
    ]


@pytest.fixture
def observations_created_response():
    return [
        {
            "object_id": "efebe106-3c50-446b-9c98-0b9b503fc922",
            "created_at": "2023-11-16T19:59:55.612864Z"
        },
        {
            "object_id": "ghebe106-3c50-446b-9c98-0b9b503fc933",
            "created_at": "2023-11-16T19:59:56.612864Z"
        }
    ]


@pytest.fixture
def auth_headers_response():
    return {
        'Accept-Type': 'application/json',
        'Authorization': 'Bearer testtoken2a97022f21732461ee103a08fac8a35'
    }


@pytest.fixture
def get_me_response():
    return {
        "username": "gundi_serviceaccount",
        "email": None,
        "first_name": "Gundi",
        "last_name": "Service Account",
        "role": "",
        "is_staff": False,
        "is_superuser": True,
        "date_joined": "2024-03-01T12:45:57.182923-08:00",
        "id": "21706d8b-98f7-4be1-bf9e-ad1639a63914",
        "is_active": True,
        "last_login": "2025-06-19T14:27:50.803093-07:00",
        "accepted_eula": True,
        "pin": None,
        "subject": None,
        "permissions": {
            "patrol": [
                "delete",
                "add",
                "view",
                "change"
            ],
            "everywherecomms_events": [
                "add",
                "view",
                "delete",
                "change"
            ],
            "patrolconfiguration": [
                "add",
                "change",
                "view",
                "delete"
            ],
            "tsvectormodel": [
                "delete",
                "view",
                "add",
                "change"
            ],
            "eventcategory": [
                "view",
                "change",
                "delete",
                "add"
            ],
            "community": [
                "add",
                "view",
                "delete",
                "change"
            ],
            "patroltype": [
                "delete",
                "view",
                "change",
                "add"
            ],
            "message": [
                "add",
                "view",
                "delete",
                "change"
            ],
            "security_2": [
                "view",
                "change",
                "add",
                "delete"
            ],
            "wildlife_detection": [
                "change",
                "view",
                "delete",
                "add"
            ],
            "team": [
                "change",
                "add",
                "view",
                "delete"
            ],
            "eventsource": [
                "add",
                "delete",
                "view",
                "change"
            ],
            "monitoring": [
                "view",
                "add",
                "delete",
                "change"
            ],
            "person": [
                "delete",
                "change",
                "add",
                "view"
            ],
            "notificationmethod": [
                "delete",
                "change",
                "add",
                "view"
            ],
            "skylight": [
                "delete",
                "add",
                "change",
                "view"
            ],
            "teammembership": [
                "view",
                "delete",
                "add",
                "change"
            ],
            "eventphoto": [
                "add",
                "view",
                "delete",
                "change"
            ],
            "eventclassfactor": [
                "add",
                "view",
                "change",
                "delete"
            ],
            "eventdetails": [
                "change",
                "add",
                "delete",
                "view"
            ],
            "patrolconfigurationsubjectgroup": [
                "view",
                "delete",
                "change",
                "add"
            ],
            "event": [
                "view",
                "delete",
                "export",
                "change",
                "add"
            ],
            "eventclass": [
                "delete",
                "change",
                "view",
                "add"
            ],
            "eventrelationshiptype": [
                "add",
                "delete",
                "change",
                "view"
            ],
            "eventnotification": [
                "delete",
                "change",
                "view",
                "add"
            ],
            "eventprovider": [
                "view",
                "change",
                "add",
                "delete"
            ],
            "alertrulenotificationmethod": [
                "add",
                "delete",
                "change",
                "view"
            ],
            "eventnote": [
                "view",
                "add",
                "change",
                "delete"
            ],
            "eventrelatedsubject": [
                "change",
                "add",
                "view",
                "delete"
            ],
            "alertruleeventtype": [
                "add",
                "change",
                "delete",
                "view"
            ],
            "membershiptype": [
                "add",
                "view",
                "delete",
                "change"
            ],
            "refreshrecreateeventdetailview": [
                "add",
                "view",
                "delete",
                "change"
            ],
            "dummy_events": [
                "delete",
                "view",
                "add",
                "change"
            ],
            "eventfactor": [
                "delete",
                "view",
                "change",
                "add"
            ],
            "eventrelatedsegments": [
                "change",
                "view",
                "delete",
                "add"
            ],
            "eventsourceevent": [
                "view",
                "add",
                "delete",
                "change"
            ],
            "formbuilderproxy": [
                "add",
                "view",
                "delete",
                "change"
            ],
            "eventgeometry": [
                "delete",
                "add",
                "change",
                "view"
            ],
            "eventtype": [
                "view",
                "delete",
                "change",
                "add"
            ],
            "eventfilter": [
                "view",
                "delete",
                "change",
                "add"
            ],
            "smart": [
                "add",
                "view",
                "delete",
                "change"
            ],
            "soilmentor_category": [
                "delete",
                "change",
                "add",
                "view"
            ],
            "eventattachment": [
                "view",
                "add",
                "change",
                "delete"
            ],
            "alertrule": [
                "add",
                "change",
                "view",
                "delete"
            ],
            "eventfile": [
                "change",
                "delete",
                "add",
                "view"
            ],
            "eventrelationship": [
                "add",
                "delete",
                "view",
                "change"
            ],
            "smart____092022": [
                "delete",
                "add",
                "view",
                "change"
            ],
            "observation": [
                "export"
            ],
            "event_for_eventsource": [
                "add"
            ]
        }
    }


@pytest.fixture
def get_event_types_response():
    return [
        {
            "id": "cdcf6a31-5d64-4a5c-8bf9-bf7d146363d0",
            "has_events_assigned": True,
            "icon": "",
            "value": "mapipedia_activity_rep",
            "display": "Ceres Tag Activity Alert",
            "ordernum": 6.103515625e-05,
            "is_collection": False,
            "category": {
                "id": "6b359461-aa53-4116-bf2c-04cc580de4ef",
                "value": "monitoring",
                "display": "Monitoring",
                "is_active": True,
                "ordernum": 6.0,
                "flag": "user",
                "permissions": [
                    "create",
                    "delete",
                    "read",
                    "update"
                ]
            },
            "icon_id": "mapipedia_activity_rep",
            "is_active": True,
            "default_priority": 100,
            "default_state": "new",
            "geometry_type": "Point",
            "resolve_time": None,
            "auto_resolve": False,
            "url": "https://gundi-dev.staging.pamdas.org/api/v1.0/activity/events/eventtypes/cdcf6a31-5d64-4a5c-8bf9-bf7d146363d0"
        },
        {
            "id": "c119f06d-a0e4-485a-af1c-af165c62317c",
            "has_events_assigned": False,
            "icon": "",
            "value": "accident_rep",
            "display": "Accident Report",
            "ordernum": 0.0001220703125,
            "is_collection": False,
            "category": {
                "id": "6b359461-aa53-4116-bf2c-04cc580de4ef",
                "value": "monitoring",
                "display": "Monitoring",
                "is_active": True,
                "ordernum": 6.0,
                "flag": "user",
                "permissions": [
                    "create",
                    "delete",
                    "read",
                    "update"
                ]
            },
            "icon_id": "accident_rep",
            "is_active": True,
            "default_priority": 0,
            "default_state": "new",
            "geometry_type": "Point",
            "resolve_time": None,
            "auto_resolve": False,
            "url": "https://gundi-dev.staging.pamdas.org/api/v1.0/activity/events/eventtypes/c119f06d-a0e4-485a-af1c-af165c62317c"
        },
        {
            "id": "0ae08721-6b7c-4d5e-aeda-cb3d1a38926f",
            "has_events_assigned": False,
            "icon": "",
            "value": "wildlife_sighting",
            "display": "Wildlife Sighting",
            "ordernum": 0.00048828125,
            "is_collection": False,
            "category": {
                "id": "6b359461-aa53-4116-bf2c-04cc580de4ef",
                "value": "monitoring",
                "display": "Monitoring",
                "is_active": True,
                "ordernum": 6.0,
                "flag": "user",
                "permissions": [
                    "create",
                    "delete",
                    "read",
                    "update"
                ]
            },
            "icon_id": "wildlife_sighting",
            "is_active": True,
            "default_priority": 100,
            "default_state": "new",
            "geometry_type": "Point",
            "resolve_time": None,
            "auto_resolve": False,
            "url": "https://gundi-dev.staging.pamdas.org/api/v1.0/activity/events/eventtypes/0ae08721-6b7c-4d5e-aeda-cb3d1a38926f",
            "readonly": True
        },
        {
            "id": "e2f88089-b422-4b13-b2b5-b08a28cd09a7",
            "has_events_assigned": False,
            "icon": "",
            "value": "sentinel_event",
            "display": "Sentinel Event",
            "ordernum": 0.0009765625,
            "is_collection": False,
            "category": {
                "id": "26cd66f4-3d96-4f33-891a-860c4d5f24cc",
                "value": "wildlife_detection",
                "display": "Wildlife Detection",
                "is_active": True,
                "ordernum": 0.015625,
                "flag": "user",
                "permissions": [
                    "create",
                    "delete",
                    "read",
                    "update"
                ]
            },
            "icon_id": "sentinel_event",
            "is_active": True,
            "default_priority": 0,
            "default_state": "new",
            "geometry_type": "Point",
            "resolve_time": None,
            "auto_resolve": False,
            "url": "https://gundi-dev.staging.pamdas.org/api/v1.0/activity/events/eventtypes/e2f88089-b422-4b13-b2b5-b08a28cd09a7"
        },
        {
            "id": "a29f303f-abed-41f3-bd0c-9e602374942d",
            "has_events_assigned": False,
            "icon": "",
            "value": "survey123_response",
            "display": "Survey 123 Response",
            "ordernum": 0.001953125,
            "is_collection": False,
            "category": {
                "id": "6b359461-aa53-4116-bf2c-04cc580de4ef",
                "value": "monitoring",
                "display": "Monitoring",
                "is_active": True,
                "ordernum": 6.0,
                "flag": "user",
                "permissions": [
                    "create",
                    "delete",
                    "read",
                    "update"
                ]
            },
            "icon_id": "survey123_response",
            "is_active": True,
            "default_priority": 0,
            "default_state": "new",
            "geometry_type": "Point",
            "resolve_time": None,
            "auto_resolve": False,
            "url": "https://gundi-dev.staging.pamdas.org/api/v1.0/activity/events/eventtypes/a29f303f-abed-41f3-bd0c-9e602374942d"
        },
        {
            "id": "fa2aa538-3902-4f4a-b0e5-c6fa142451b5",
            "has_events_assigned": False,
            "icon": "",
            "value": "soilmentor_soiltest",
            "display": "Soilmentor",
            "ordernum": 0.00390625,
            "is_collection": False,
            "category": {
                "id": "1d7a6f83-50f1-4426-85b3-4b8606d384e4",
                "value": "soilmentor_category",
                "display": "Soilmentor Events",
                "is_active": True,
                "ordernum": 0.03125,
                "flag": "user",
                "permissions": [
                    "create",
                    "delete",
                    "read",
                    "update"
                ]
            },
            "icon_id": "soilmentor_soiltest",
            "is_active": True,
            "default_priority": 100,
            "default_state": "new",
            "geometry_type": "Point",
            "resolve_time": None,
            "auto_resolve": False,
            "url": "https://gundi-dev.staging.pamdas.org/api/v1.0/activity/events/eventtypes/fa2aa538-3902-4f4a-b0e5-c6fa142451b5",
            "readonly": True
        },
        {
            "id": "add478dd-cafb-48ce-9e7e-9c10c6cf3576",
            "has_events_assigned": False,
            "icon": "",
            "value": "sit_rep",
            "display": "Sit Rep",
            "ordernum": 0.0078125,
            "is_collection": False,
            "category": {
                "id": "02fa4ba7-6395-4809-94de-fab17047b09a",
                "value": "security_2",
                "display": "Security",
                "is_active": True,
                "ordernum": 2.0,
                "flag": "user",
                "permissions": [
                    "create",
                    "delete",
                    "read",
                    "update"
                ]
            },
            "icon_id": "sit_rep",
            "is_active": True,
            "default_priority": 0,
            "default_state": "new",
            "geometry_type": "Point",
            "resolve_time": None,
            "auto_resolve": False,
            "url": "https://gundi-dev.staging.pamdas.org/api/v1.0/activity/events/eventtypes/add478dd-cafb-48ce-9e7e-9c10c6cf3576"
        },
        {
            "id": "ac537911-38cb-4b4c-8b41-007bc3d8b3a1",
            "has_events_assigned": False,
            "icon": "",
            "value": "activity_rep",
            "display": "Activity Alert",
            "ordernum": 0.015625,
            "is_collection": False,
            "category": {
                "id": "6b359461-aa53-4116-bf2c-04cc580de4ef",
                "value": "monitoring",
                "display": "Monitoring",
                "is_active": True,
                "ordernum": 6.0,
                "flag": "user",
                "permissions": [
                    "create",
                    "delete",
                    "read",
                    "update"
                ]
            },
            "icon_id": "activity_rep",
            "is_active": True,
            "default_priority": 300,
            "default_state": "new",
            "geometry_type": "Point",
            "resolve_time": None,
            "auto_resolve": False,
            "url": "https://gundi-dev.staging.pamdas.org/api/v1.0/activity/events/eventtypes/ac537911-38cb-4b4c-8b41-007bc3d8b3a1"
        },
        {
            "id": "797d8aa3-7dda-4701-ae22-9d0166afd8a8",
            "has_events_assigned": False,
            "icon": "",
            "value": "iap_area",
            "display": "IAP Area",
            "ordernum": 0.03125,
            "is_collection": False,
            "category": {
                "id": "6b359461-aa53-4116-bf2c-04cc580de4ef",
                "value": "monitoring",
                "display": "Monitoring",
                "is_active": True,
                "ordernum": 6.0,
                "flag": "user",
                "permissions": [
                    "create",
                    "delete",
                    "read",
                    "update"
                ]
            },
            "icon_id": "iap_area",
            "is_active": True,
            "default_priority": 200,
            "default_state": "new",
            "geometry_type": "Polygon",
            "resolve_time": None,
            "auto_resolve": False,
            "url": "https://gundi-dev.staging.pamdas.org/api/v1.0/activity/events/eventtypes/797d8aa3-7dda-4701-ae22-9d0166afd8a8"
        },
        {
            "id": "c56261e0-5494-40b4-9d21-746a42892ad9",
            "has_events_assigned": True,
            "icon": "natural_event_rep",
            "value": "weather_station_summary",
            "display": "Weather Station Summary",
            "ordernum": 0.0625,
            "is_collection": False,
            "category": {
                "id": "6b359461-aa53-4116-bf2c-04cc580de4ef",
                "value": "monitoring",
                "display": "Monitoring",
                "is_active": True,
                "ordernum": 6.0,
                "flag": "user",
                "permissions": [
                    "create",
                    "delete",
                    "read",
                    "update"
                ]
            },
            "icon_id": "natural_event_rep",
            "is_active": True,
            "default_priority": 100,
            "default_state": "new",
            "geometry_type": "Point",
            "resolve_time": None,
            "auto_resolve": False,
            "url": "https://gundi-dev.staging.pamdas.org/api/v1.0/activity/events/eventtypes/c56261e0-5494-40b4-9d21-746a42892ad9"
        },
        {
            "id": "74cddb6e-a1ec-452b-b658-3a5e9e906df5",
            "has_events_assigned": False,
            "icon": "fire_rep",
            "value": "ororatechalert_withgeometry",
            "display": "Ororatech Fire",
            "ordernum": 0.125,
            "is_collection": False,
            "category": {
                "id": "4f874b28-0127-44e1-9b10-1f78817a9a3f",
                "value": "analyzer_event",
                "display": "Analyzer Event",
                "is_active": True,
                "ordernum": 3.0,
                "flag": "system",
                "permissions": [
                    "create",
                    "delete",
                    "read",
                    "update"
                ]
            },
            "icon_id": "fire_rep",
            "is_active": True,
            "default_priority": 0,
            "default_state": "new",
            "geometry_type": "Polygon",
            "resolve_time": None,
            "auto_resolve": False,
            "url": "https://gundi-dev.staging.pamdas.org/api/v1.0/activity/events/eventtypes/74cddb6e-a1ec-452b-b658-3a5e9e906df5"
        },
        {
            "id": "f721c002-3404-4fe7-8d71-64200800ec90",
            "has_events_assigned": False,
            "icon": "",
            "value": "immobility_all_clear",
            "display": "Mortality All Clear",
            "ordernum": 0.25,
            "is_collection": False,
            "category": {
                "id": "6b359461-aa53-4116-bf2c-04cc580de4ef",
                "value": "monitoring",
                "display": "Monitoring",
                "is_active": True,
                "ordernum": 6.0,
                "flag": "user",
                "permissions": [
                    "create",
                    "delete",
                    "read",
                    "update"
                ]
            },
            "icon_id": "immobility_all_clear",
            "is_active": True,
            "default_priority": 100,
            "default_state": "new",
            "geometry_type": "Point",
            "resolve_time": None,
            "auto_resolve": False,
            "url": "https://gundi-dev.staging.pamdas.org/api/v1.0/activity/events/eventtypes/f721c002-3404-4fe7-8d71-64200800ec90"
        },
        {
            "id": "5a20a873-669e-43bc-8bea-9858bde21024",
            "has_events_assigned": False,
            "icon": "",
            "value": "immobility",
            "display": "Mortality Alert",
            "ordernum": 0.5,
            "is_collection": False,
            "category": {
                "id": "6b359461-aa53-4116-bf2c-04cc580de4ef",
                "value": "monitoring",
                "display": "Monitoring",
                "is_active": True,
                "ordernum": 6.0,
                "flag": "user",
                "permissions": [
                    "create",
                    "delete",
                    "read",
                    "update"
                ]
            },
            "icon_id": "immobility",
            "is_active": True,
            "default_priority": 100,
            "default_state": "new",
            "geometry_type": "Point",
            "resolve_time": None,
            "auto_resolve": False,
            "url": "https://gundi-dev.staging.pamdas.org/api/v1.0/activity/events/eventtypes/5a20a873-669e-43bc-8bea-9858bde21024"
        },
        {
            "id": "5122e2e4-7d1f-4336-87a5-d7c9b26c65ed",
            "has_events_assigned": False,
            "icon": "",
            "value": "ew_self_check_in_stopped",
            "display": "Self Check-in Stopped",
            "ordernum": 2.0,
            "is_collection": False,
            "category": {
                "id": "943a5426-9b5e-49b9-a8ac-6d512c68b5db",
                "value": "everywherecomms_events",
                "display": "Everywhere Communications Events",
                "is_active": True,
                "ordernum": 0.0625,
                "flag": "user",
                "permissions": [
                    "create",
                    "delete",
                    "read",
                    "update"
                ]
            },
            "icon_id": "ew_self_check_in_stopped",
            "is_active": True,
            "default_priority": 200,
            "default_state": "new",
            "geometry_type": "Point",
            "resolve_time": None,
            "auto_resolve": False,
            "url": "https://gundi-dev.staging.pamdas.org/api/v1.0/activity/events/eventtypes/5122e2e4-7d1f-4336-87a5-d7c9b26c65ed"
        },
        {
            "id": "bfe80573-a192-4fb1-9ddd-5b23481b0c69",
            "has_events_assigned": False,
            "icon": "",
            "value": "ew_self_check_in_started",
            "display": "Self Check-in Started",
            "ordernum": 3.0,
            "is_collection": False,
            "category": {
                "id": "943a5426-9b5e-49b9-a8ac-6d512c68b5db",
                "value": "everywherecomms_events",
                "display": "Everywhere Communications Events",
                "is_active": True,
                "ordernum": 0.0625,
                "flag": "user",
                "permissions": [
                    "create",
                    "delete",
                    "read",
                    "update"
                ]
            },
            "icon_id": "ew_self_check_in_started",
            "is_active": True,
            "default_priority": 100,
            "default_state": "new",
            "geometry_type": "Point",
            "resolve_time": None,
            "auto_resolve": False,
            "url": "https://gundi-dev.staging.pamdas.org/api/v1.0/activity/events/eventtypes/bfe80573-a192-4fb1-9ddd-5b23481b0c69"
        },
        {
            "id": "0745651c-a8ba-4e50-8a8b-989250b0f89a",
            "has_events_assigned": False,
            "icon": "",
            "value": "ew_missed_check_in_escalation",
            "display": "Missed Check-in Escalation",
            "ordernum": 4.0,
            "is_collection": False,
            "category": {
                "id": "943a5426-9b5e-49b9-a8ac-6d512c68b5db",
                "value": "everywherecomms_events",
                "display": "Everywhere Communications Events",
                "is_active": True,
                "ordernum": 0.0625,
                "flag": "user",
                "permissions": [
                    "create",
                    "delete",
                    "read",
                    "update"
                ]
            },
            "icon_id": "ew_missed_check_in_escalation",
            "is_active": True,
            "default_priority": 300,
            "default_state": "new",
            "geometry_type": "Point",
            "resolve_time": None,
            "auto_resolve": False,
            "url": "https://gundi-dev.staging.pamdas.org/api/v1.0/activity/events/eventtypes/0745651c-a8ba-4e50-8a8b-989250b0f89a"
        },
        {
            "id": "12fba557-ec5b-426b-a583-bdb126f383ba",
            "has_events_assigned": False,
            "icon": "",
            "value": "ew_missed_check_in",
            "display": "Missed Check-in",
            "ordernum": 5.0,
            "is_collection": False,
            "category": {
                "id": "943a5426-9b5e-49b9-a8ac-6d512c68b5db",
                "value": "everywherecomms_events",
                "display": "Everywhere Communications Events",
                "is_active": True,
                "ordernum": 0.0625,
                "flag": "user",
                "permissions": [
                    "create",
                    "delete",
                    "read",
                    "update"
                ]
            },
            "icon_id": "ew_missed_check_in",
            "is_active": True,
            "default_priority": 200,
            "default_state": "new",
            "geometry_type": "Point",
            "resolve_time": None,
            "auto_resolve": False,
            "url": "https://gundi-dev.staging.pamdas.org/api/v1.0/activity/events/eventtypes/12fba557-ec5b-426b-a583-bdb126f383ba"
        },
        {
            "id": "a1b1d5b9-d31d-4bf9-a543-589006159625",
            "has_events_assigned": True,
            "icon": "",
            "value": "ew_geofence_exited",
            "display": "Geofence Exited",
            "ordernum": 6.0,
            "is_collection": False,
            "category": {
                "id": "943a5426-9b5e-49b9-a8ac-6d512c68b5db",
                "value": "everywherecomms_events",
                "display": "Everywhere Communications Events",
                "is_active": True,
                "ordernum": 0.0625,
                "flag": "user",
                "permissions": [
                    "create",
                    "delete",
                    "read",
                    "update"
                ]
            },
            "icon_id": "ew_geofence_exited",
            "is_active": True,
            "default_priority": 200,
            "default_state": "new",
            "geometry_type": "Point",
            "resolve_time": None,
            "auto_resolve": False,
            "url": "https://gundi-dev.staging.pamdas.org/api/v1.0/activity/events/eventtypes/a1b1d5b9-d31d-4bf9-a543-589006159625"
        },
        {
            "id": "f4184592-3a50-46c8-8109-6513772baa94",
            "has_events_assigned": False,
            "icon": "",
            "value": "ew_factal_news_alert",
            "display": "News Alert (FACTAL)",
            "ordernum": 6.5,
            "is_collection": False,
            "category": {
                "id": "943a5426-9b5e-49b9-a8ac-6d512c68b5db",
                "value": "everywherecomms_events",
                "display": "Everywhere Communications Events",
                "is_active": True,
                "ordernum": 0.0625,
                "flag": "user",
                "permissions": [
                    "create",
                    "delete",
                    "read",
                    "update"
                ]
            },
            "icon_id": "ew_factal_news_alert",
            "is_active": True,
            "default_priority": 300,
            "default_state": "new",
            "geometry_type": "Point",
            "resolve_time": None,
            "auto_resolve": False,
            "url": "https://gundi-dev.staging.pamdas.org/api/v1.0/activity/events/eventtypes/f4184592-3a50-46c8-8109-6513772baa94"
        },
        {
            "id": "66921eb9-fd54-48ff-8c0f-bfd45ba32984",
            "has_events_assigned": False,
            "icon": "",
            "value": "ew_geofence_entered",
            "display": "Geofence Entered",
            "ordernum": 7.0,
            "is_collection": False,
            "category": {
                "id": "943a5426-9b5e-49b9-a8ac-6d512c68b5db",
                "value": "everywherecomms_events",
                "display": "Everywhere Communications Events",
                "is_active": True,
                "ordernum": 0.0625,
                "flag": "user",
                "permissions": [
                    "create",
                    "delete",
                    "read",
                    "update"
                ]
            },
            "icon_id": "ew_geofence_entered",
            "is_active": True,
            "default_priority": 200,
            "default_state": "new",
            "geometry_type": "Point",
            "resolve_time": None,
            "auto_resolve": False,
            "url": "https://gundi-dev.staging.pamdas.org/api/v1.0/activity/events/eventtypes/66921eb9-fd54-48ff-8c0f-bfd45ba32984"
        },
        {
            "id": "9844ff7d-df23-480c-ba35-2e61ed7ea6b5",
            "has_events_assigned": False,
            "icon": "",
            "value": "ew_emergency_exited",
            "display": "Emergency Exited",
            "ordernum": 9.0,
            "is_collection": False,
            "category": {
                "id": "943a5426-9b5e-49b9-a8ac-6d512c68b5db",
                "value": "everywherecomms_events",
                "display": "Everywhere Communications Events",
                "is_active": True,
                "ordernum": 0.0625,
                "flag": "user",
                "permissions": [
                    "create",
                    "delete",
                    "read",
                    "update"
                ]
            },
            "icon_id": "ew_emergency_exited",
            "is_active": True,
            "default_priority": 200,
            "default_state": "new",
            "geometry_type": "Point",
            "resolve_time": None,
            "auto_resolve": False,
            "url": "https://gundi-dev.staging.pamdas.org/api/v1.0/activity/events/eventtypes/9844ff7d-df23-480c-ba35-2e61ed7ea6b5"
        },
        {
            "id": "a3263ef2-744a-4b48-8bd1-58ea07133f98",
            "has_events_assigned": False,
            "icon": "",
            "value": "ew_emergency_entered",
            "display": "Emergency Entered",
            "ordernum": 10.0,
            "is_collection": False,
            "category": {
                "id": "943a5426-9b5e-49b9-a8ac-6d512c68b5db",
                "value": "everywherecomms_events",
                "display": "Everywhere Communications Events",
                "is_active": True,
                "ordernum": 0.0625,
                "flag": "user",
                "permissions": [
                    "create",
                    "delete",
                    "read",
                    "update"
                ]
            },
            "icon_id": "ew_emergency_entered",
            "is_active": True,
            "default_priority": 200,
            "default_state": "new",
            "geometry_type": "Point",
            "resolve_time": None,
            "auto_resolve": False,
            "url": "https://gundi-dev.staging.pamdas.org/api/v1.0/activity/events/eventtypes/a3263ef2-744a-4b48-8bd1-58ea07133f98"
        },
        {
            "id": "78ac35e7-64d5-4f4a-9288-ee0826c35b8d",
            "has_events_assigned": False,
            "icon": "",
            "value": "ew_check_in_not_ok",
            "display": "Check-in not OK",
            "ordernum": 11.0,
            "is_collection": False,
            "category": {
                "id": "943a5426-9b5e-49b9-a8ac-6d512c68b5db",
                "value": "everywherecomms_events",
                "display": "Everywhere Communications Events",
                "is_active": True,
                "ordernum": 0.0625,
                "flag": "user",
                "permissions": [
                    "create",
                    "delete",
                    "read",
                    "update"
                ]
            },
            "icon_id": "ew_check_in_not_ok",
            "is_active": True,
            "default_priority": 300,
            "default_state": "new",
            "geometry_type": "Point",
            "resolve_time": None,
            "auto_resolve": False,
            "url": "https://gundi-dev.staging.pamdas.org/api/v1.0/activity/events/eventtypes/78ac35e7-64d5-4f4a-9288-ee0826c35b8d"
        },
        {
            "id": "2c4903ac-4ae5-47d6-ac0c-2e6d0290cb3c",
            "has_events_assigned": False,
            "icon": "",
            "value": "ew_check_in_im_ok",
            "display": "Check in I'm OK",
            "ordernum": 12.0,
            "is_collection": False,
            "category": {
                "id": "943a5426-9b5e-49b9-a8ac-6d512c68b5db",
                "value": "everywherecomms_events",
                "display": "Everywhere Communications Events",
                "is_active": True,
                "ordernum": 0.0625,
                "flag": "user",
                "permissions": [
                    "create",
                    "delete",
                    "read",
                    "update"
                ]
            },
            "icon_id": "ew_check_in_im_ok",
            "is_active": True,
            "default_priority": 100,
            "default_state": "new",
            "geometry_type": "Point",
            "resolve_time": None,
            "auto_resolve": False,
            "url": "https://gundi-dev.staging.pamdas.org/api/v1.0/activity/events/eventtypes/2c4903ac-4ae5-47d6-ac0c-2e6d0290cb3c"
        },
        {
            "id": "e1ebf15b-9049-4c11-8024-47e7333dc9a8",
            "has_events_assigned": False,
            "icon": "skylight-speed_range",
            "value": "speed_range_alert_rep",
            "display": "Speed Alert",
            "ordernum": 13.0,
            "is_collection": False,
            "category": {
                "id": "6b359461-aa53-4116-bf2c-04cc580de4ef",
                "value": "monitoring",
                "display": "Monitoring",
                "is_active": True,
                "ordernum": 6.0,
                "flag": "user",
                "permissions": [
                    "create",
                    "delete",
                    "read",
                    "update"
                ]
            },
            "icon_id": "skylight-speed_range",
            "is_active": True,
            "default_priority": 100,
            "default_state": "active",
            "geometry_type": "Point",
            "resolve_time": None,
            "auto_resolve": False,
            "url": "https://gundi-dev.staging.pamdas.org/api/v1.0/activity/events/eventtypes/e1ebf15b-9049-4c11-8024-47e7333dc9a8"
        },
        {
            "id": "a9c8b7eb-0015-4af4-8785-ea95275b09ce",
            "has_events_assigned": False,
            "icon": "skylight-entry_rep",
            "value": "detection_alert_rep",
            "display": "Vesel Detection SL",
            "ordernum": 42.0,
            "is_collection": False,
            "category": {
                "id": "02fa4ba7-6395-4809-94de-fab17047b09a",
                "value": "security_2",
                "display": "Security",
                "is_active": True,
                "ordernum": 2.0,
                "flag": "user",
                "permissions": [
                    "create",
                    "delete",
                    "read",
                    "update"
                ]
            },
            "icon_id": "skylight-entry_rep",
            "is_active": True,
            "default_priority": 0,
            "default_state": "new",
            "geometry_type": "Point",
            "resolve_time": None,
            "auto_resolve": False,
            "url": "https://gundi-dev.staging.pamdas.org/api/v1.0/activity/events/eventtypes/a9c8b7eb-0015-4af4-8785-ea95275b09ce"
        },
        {
            "id": "8a625765-7299-4b1b-8756-c8bbb0cf6ef3",
            "has_events_assigned": False,
            "icon": "observation-patrol-icon",
            "value": "ebird_observation",
            "display": "eBird Observations",
            "ordernum": 43.0,
            "is_collection": False,
            "category": {
                "id": "6b359461-aa53-4116-bf2c-04cc580de4ef",
                "value": "monitoring",
                "display": "Monitoring",
                "is_active": True,
                "ordernum": 6.0,
                "flag": "user",
                "permissions": [
                    "create",
                    "delete",
                    "read",
                    "update"
                ]
            },
            "icon_id": "observation-patrol-icon",
            "is_active": True,
            "default_priority": 100,
            "default_state": "active",
            "geometry_type": "Point",
            "resolve_time": None,
            "auto_resolve": False,
            "url": "https://gundi-dev.staging.pamdas.org/api/v1.0/activity/events/eventtypes/8a625765-7299-4b1b-8756-c8bbb0cf6ef3"
        },
        {
            "id": "fe609c11-d622-4a90-bda5-79db164e8bb9",
            "has_events_assigned": False,
            "icon": "",
            "value": "wpswatch_rep",
            "display": "wpsWatch Integration",
            "ordernum": 44.0,
            "is_collection": False,
            "category": {
                "id": "6b359461-aa53-4116-bf2c-04cc580de4ef",
                "value": "monitoring",
                "display": "Monitoring",
                "is_active": True,
                "ordernum": 6.0,
                "flag": "user",
                "permissions": [
                    "create",
                    "delete",
                    "read",
                    "update"
                ]
            },
            "icon_id": "wpswatch_rep",
            "is_active": True,
            "default_priority": 0,
            "default_state": "new",
            "geometry_type": "Point",
            "resolve_time": None,
            "auto_resolve": False,
            "url": "https://gundi-dev.staging.pamdas.org/api/v1.0/activity/events/eventtypes/fe609c11-d622-4a90-bda5-79db164e8bb9"
        },
        {
            "id": "783d5214-da6a-40fc-8466-9b7642ac34c2",
            "has_events_assigned": False,
            "icon": "",
            "value": "cameratrap_rep",
            "display": "Camera Trap",
            "ordernum": 45.0,
            "is_collection": False,
            "category": {
                "id": "02fa4ba7-6395-4809-94de-fab17047b09a",
                "value": "security_2",
                "display": "Security",
                "is_active": True,
                "ordernum": 2.0,
                "flag": "user",
                "permissions": [
                    "create",
                    "delete",
                    "read",
                    "update"
                ]
            },
            "icon_id": "cameratrap_rep",
            "is_active": True,
            "default_priority": 0,
            "default_state": "new",
            "geometry_type": "Point",
            "resolve_time": None,
            "auto_resolve": False,
            "url": "https://gundi-dev.staging.pamdas.org/api/v1.0/activity/events/eventtypes/783d5214-da6a-40fc-8466-9b7642ac34c2"
        },
        {
            "id": "a3ddff60-4bd6-4719-bb0f-9b54a88e64bf",
            "has_events_assigned": False,
            "icon": "cameratrap_rep",
            "value": "inaturalist",
            "display": "iNaturalist",
            "ordernum": 46.0,
            "is_collection": False,
            "category": {
                "id": "6b359461-aa53-4116-bf2c-04cc580de4ef",
                "value": "monitoring",
                "display": "Monitoring",
                "is_active": True,
                "ordernum": 6.0,
                "flag": "user",
                "permissions": [
                    "create",
                    "delete",
                    "read",
                    "update"
                ]
            },
            "icon_id": "cameratrap_rep",
            "is_active": True,
            "default_priority": 0,
            "default_state": "new",
            "geometry_type": "Point",
            "resolve_time": None,
            "auto_resolve": False,
            "url": "https://gundi-dev.staging.pamdas.org/api/v1.0/activity/events/eventtypes/a3ddff60-4bd6-4719-bb0f-9b54a88e64bf"
        },
        {
            "id": "47ce7ada-030d-4b30-9ed0-1358245f0c3a",
            "has_events_assigned": False,
            "icon": "fire_rep",
            "value": "gfwfirealert",
            "display": "GFW Fire Alert",
            "ordernum": 47.0,
            "is_collection": False,
            "category": {
                "id": "4f874b28-0127-44e1-9b10-1f78817a9a3f",
                "value": "analyzer_event",
                "display": "Analyzer Event",
                "is_active": True,
                "ordernum": 3.0,
                "flag": "system",
                "permissions": [
                    "create",
                    "delete",
                    "read",
                    "update"
                ]
            },
            "icon_id": "fire_rep",
            "is_active": True,
            "default_priority": 300,
            "default_state": "new",
            "geometry_type": "Point",
            "resolve_time": None,
            "auto_resolve": False,
            "url": "https://gundi-dev.staging.pamdas.org/api/v1.0/activity/events/eventtypes/47ce7ada-030d-4b30-9ed0-1358245f0c3a"
        },
        {
            "id": "ada73f4d-de63-47b4-93e7-837bec1af9f2",
            "has_events_assigned": False,
            "icon": "deforestation_rep",
            "value": "gfwgladalert",
            "display": "GFW Integrated Alerts",
            "ordernum": 48.0,
            "is_collection": False,
            "category": {
                "id": "4f874b28-0127-44e1-9b10-1f78817a9a3f",
                "value": "analyzer_event",
                "display": "Analyzer Event",
                "is_active": True,
                "ordernum": 3.0,
                "flag": "system",
                "permissions": [
                    "create",
                    "delete",
                    "read",
                    "update"
                ]
            },
            "icon_id": "deforestation_rep",
            "is_active": True,
            "default_priority": 200,
            "default_state": "active",
            "geometry_type": "Point",
            "resolve_time": None,
            "auto_resolve": False,
            "url": "https://gundi-dev.staging.pamdas.org/api/v1.0/activity/events/eventtypes/ada73f4d-de63-47b4-93e7-837bec1af9f2",
            "readonly": True
        },
        {
            "id": "61151b0d-684c-406f-b97c-12dca6a985ec",
            "has_events_assigned": False,
            "icon": "",
            "value": "entry_alert_rep",
            "display": "Entry Alert",
            "ordernum": 49.0,
            "is_collection": False,
            "category": {
                "id": "3592d610-c92a-44f3-b613-6345cbda6e12",
                "value": "skylight",
                "display": "Skylight",
                "is_active": True,
                "ordernum": 0.5,
                "flag": "user",
                "permissions": [
                    "create",
                    "delete",
                    "read",
                    "update"
                ]
            },
            "icon_id": "entry_alert_rep",
            "is_active": True,
            "default_priority": 200,
            "default_state": "new",
            "geometry_type": "Point",
            "resolve_time": None,
            "auto_resolve": False,
            "url": "https://gundi-dev.staging.pamdas.org/api/v1.0/activity/events/eventtypes/61151b0d-684c-406f-b97c-12dca6a985ec"
        },
        {
            "id": "42a57bf1-82e7-45e2-95b2-39357687694f",
            "has_events_assigned": False,
            "icon": None,
            "value": "rainfall_rep",
            "display": "Rainfall",
            "ordernum": 50.0,
            "is_collection": False,
            "category": {
                "id": "6b359461-aa53-4116-bf2c-04cc580de4ef",
                "value": "monitoring",
                "display": "Monitoring",
                "is_active": True,
                "ordernum": 6.0,
                "flag": "user",
                "permissions": [
                    "create",
                    "delete",
                    "read",
                    "update"
                ]
            },
            "icon_id": "rainfall_rep",
            "is_active": True,
            "default_priority": 0,
            "default_state": "new",
            "geometry_type": "Point",
            "resolve_time": None,
            "auto_resolve": False,
            "url": "https://gundi-dev.staging.pamdas.org/api/v1.0/activity/events/eventtypes/42a57bf1-82e7-45e2-95b2-39357687694f"
        }
    ]


@pytest.fixture
def get_subjectgroups_response():
    return [
        {
            "id": "07200222-9f6b-49ed-b390-c3c903948702",
            "name": "Telonics",
            "subjects": [],
            "subgroups": []
        },
        {
            "id": "286fc226-095d-407f-9b17-b715761abdca",
            "name": "Stevens Connect",
            "subjects": [],
            "subgroups": []
        },
        {
            "id": "28e5d22d-dc91-4160-9328-d5e5cd768ec6",
            "name": "Vital Weather",
            "subjects": [],
            "subgroups": []
        },
        {
            "id": "3ae98788-ee22-4283-bbd1-683872008f83",
            "name": "AWT",
            "subjects": [
                {
                    "content_type": "observations.subject",
                    "id": "bc410952-86f0-4de6-93f6-e4b2c548872c",
                    "name": "00001",
                    "subject_type": "wildlife",
                    "subject_subtype": "giraffe",
                    "common_name": None,
                    "additional": {},
                    "created_at": "2025-08-25T15:28:29.717747-07:00",
                    "updated_at": "2025-08-25T15:30:46.181921-07:00",
                    "is_active": True,
                    "user": None,
                    "tracks_available": True,
                    "image_url": "/static/giraffe-male.svg",
                    "last_position_status": {
                        "last_voice_call_start_at": None,
                        "radio_state_at": None,
                        "radio_state": "na"
                    },
                    "last_position_date": "2025-08-23T15:01:02+00:00",
                    "last_position": {
                        "type": "Feature",
                        "geometry": {
                            "type": "Point",
                            "coordinates": [
                                -123.456,
                                47.123
                            ]
                        },
                        "properties": {
                            "title": "00001",
                            "subject_type": "wildlife",
                            "subject_subtype": "giraffe",
                            "id": "bc410952-86f0-4de6-93f6-e4b2c548872c",
                            "stroke": "#FFFF00",
                            "stroke-opacity": 1.0,
                            "stroke-width": 2,
                            "image": "https://gundi-dev.staging.pamdas.org/static/giraffe-male.svg",
                            "last_voice_call_start_at": None,
                            "location_requested_at": None,
                            "radio_state_at": "1970-01-01T00:00:00+00:00",
                            "radio_state": "na",
                            "coordinateProperties": {
                                "time": "2025-08-23T15:01:02+00:00"
                            },
                            "DateTime": "2025-08-23T15:01:02+00:00"
                        }
                    },
                    "device_status_properties": [
                        {
                            "label": "activity",
                            "units": None,
                            "value": 7
                        },
                        {
                            "label": "location_accuracy",
                            "units": None,
                            "value": 3
                        },
                        {
                            "label": "temperature",
                            "units": "C",
                            "value": 15
                        },
                        {
                            "label": "battery",
                            "units": "V",
                            "value": 32
                        }
                    ],
                    "url": "https://gundi-dev.staging.pamdas.org/api/v1.0/subject/bc410952-86f0-4de6-93f6-e4b2c548872c"
                }
            ],
            "subgroups": []
        },
        {
            "id": "a9672a01-c05a-4081-932f-4178c6124899",
            "name": "Seattle Radios",
            "subjects": [],
            "subgroups": []
        },
        {
            "id": "b36ccafa-6651-4346-96b5-374b2b7b8166",
            "name": "DigitAnimal (Sintra Cascais Ambiente)",
            "subjects": [],
            "subgroups": []
        },
        {
            "id": "bc8f7652-31e3-47fe-a185-060a18a23059",
            "name": "MMSubjectsWithSubGroup",
            "subjects": [],
            "subgroups": [
                {
                    "id": "ccf85a2b-2e2e-4015-a2ff-135cf1e166c8",
                    "name": "MMSubjectsSub",
                    "subjects": [
                        {
                            "content_type": "observations.subject",
                            "id": "51170be3-0ec1-4838-be07-ca9ad9954cbe",
                            "name": "MM Truck",
                            "subject_type": "unassigned",
                            "subject_subtype": "vehicle",
                            "common_name": None,
                            "additional": {},
                            "created_at": "2025-08-26T10:53:23.705391-07:00",
                            "updated_at": "2025-08-26T10:53:23.705428-07:00",
                            "is_active": True,
                            "user": None,
                            "tracks_available": False,
                            "image_url": "/static/pin-black.svg",
                            "last_position_status": {
                                "last_voice_call_start_at": None,
                                "radio_state_at": None,
                                "radio_state": "na"
                            },
                            "device_status_properties": None,
                            "url": "https://gundi-dev.staging.pamdas.org/api/v1.0/subject/51170be3-0ec1-4838-be07-ca9ad9954cbe"
                        },
                        {
                            "content_type": "observations.subject",
                            "id": "b9893e89-71c0-4d99-bff2-50e25eec2fe5",
                            "name": "Jeep X",
                            "subject_type": "unassigned",
                            "subject_subtype": "vehicle",
                            "common_name": None,
                            "additional": {},
                            "created_at": "2025-08-26T10:52:58.292713-07:00",
                            "updated_at": "2025-08-26T10:52:58.292740-07:00",
                            "is_active": True,
                            "user": None,
                            "tracks_available": False,
                            "image_url": "/static/pin-black.svg",
                            "last_position_status": {
                                "last_voice_call_start_at": None,
                                "radio_state_at": None,
                                "radio_state": "na"
                            },
                            "device_status_properties": None,
                            "url": "https://gundi-dev.staging.pamdas.org/api/v1.0/subject/b9893e89-71c0-4d99-bff2-50e25eec2fe5"
                        }
                    ],
                    "subgroups": [
                        {
                            "id": "f776fed7-0666-4146-a405-b2a0bd3ea274",
                            "name": "MMSubjectsSubSub",
                            "subjects": [
                                {
                                    "content_type": "observations.subject",
                                    "id": "f8cb9fa2-55cb-4ef7-b2f6-2044d60c7606",
                                    "name": "Mariano",
                                    "subject_type": "person",
                                    "subject_subtype": "ranger",
                                    "common_name": None,
                                    "additional": {},
                                    "created_at": "2025-08-26T10:53:44.452504-07:00",
                                    "updated_at": "2025-08-26T10:53:44.452534-07:00",
                                    "is_active": True,
                                    "user": None,
                                    "tracks_available": False,
                                    "image_url": "/static/ranger-black.svg",
                                    "last_position_status": {
                                        "last_voice_call_start_at": None,
                                        "radio_state_at": None,
                                        "radio_state": "na"
                                    },
                                    "device_status_properties": None,
                                    "url": "https://gundi-dev.staging.pamdas.org/api/v1.0/subject/f8cb9fa2-55cb-4ef7-b2f6-2044d60c7606"
                                }
                            ],
                            "subgroups": []
                        }
                    ]
                }
            ]
        },
        {
            "id": "ca482c7a-f4c9-419a-9499-f107cc217c7c",
            "name": "Marine Monitor",
            "subjects": [],
            "subgroups": []
        },
        {
            "id": "ff039002-00af-48ad-acf4-218373991a07",
            "name": "Subjects",
            "subjects": [
                {
                    "content_type": "observations.subject",
                    "id": "f8cb9fa2-55cb-4ef7-b2f6-2044d60c7606",
                    "name": "Mariano",
                    "subject_type": "person",
                    "subject_subtype": "ranger",
                    "common_name": None,
                    "additional": {},
                    "created_at": "2025-08-26T10:53:44.452504-07:00",
                    "updated_at": "2025-08-26T10:53:44.452534-07:00",
                    "is_active": True,
                    "user": None,
                    "tracks_available": False,
                    "image_url": "/static/ranger-black.svg",
                    "last_position_status": {
                        "last_voice_call_start_at": None,
                        "radio_state_at": None,
                        "radio_state": "na"
                    },
                    "device_status_properties": None,
                    "url": "https://gundi-dev.staging.pamdas.org/api/v1.0/subject/f8cb9fa2-55cb-4ef7-b2f6-2044d60c7606"
                },
                {
                    "content_type": "observations.subject",
                    "id": "51170be3-0ec1-4838-be07-ca9ad9954cbe",
                    "name": "MM Truck",
                    "subject_type": "unassigned",
                    "subject_subtype": "vehicle",
                    "common_name": None,
                    "additional": {},
                    "created_at": "2025-08-26T10:53:23.705391-07:00",
                    "updated_at": "2025-08-26T10:53:23.705428-07:00",
                    "is_active": True,
                    "user": None,
                    "tracks_available": False,
                    "image_url": "/static/pin-black.svg",
                    "last_position_status": {
                        "last_voice_call_start_at": None,
                        "radio_state_at": None,
                        "radio_state": "na"
                    },
                    "device_status_properties": None,
                    "url": "https://gundi-dev.staging.pamdas.org/api/v1.0/subject/51170be3-0ec1-4838-be07-ca9ad9954cbe"
                },
                {
                    "content_type": "observations.subject",
                    "id": "b9893e89-71c0-4d99-bff2-50e25eec2fe5",
                    "name": "Jeep X",
                    "subject_type": "unassigned",
                    "subject_subtype": "vehicle",
                    "common_name": None,
                    "additional": {},
                    "created_at": "2025-08-26T10:52:58.292713-07:00",
                    "updated_at": "2025-08-26T10:52:58.292740-07:00",
                    "is_active": True,
                    "user": None,
                    "tracks_available": False,
                    "image_url": "/static/pin-black.svg",
                    "last_position_status": {
                        "last_voice_call_start_at": None,
                        "radio_state_at": None,
                        "radio_state": "na"
                    },
                    "device_status_properties": None,
                    "url": "https://gundi-dev.staging.pamdas.org/api/v1.0/subject/b9893e89-71c0-4d99-bff2-50e25eec2fe5"
                },
                {
                    "content_type": "observations.subject",
                    "id": "bc410952-86f0-4de6-93f6-e4b2c548872c",
                    "name": "00001",
                    "subject_type": "wildlife",
                    "subject_subtype": "giraffe",
                    "common_name": None,
                    "additional": {},
                    "created_at": "2025-08-25T15:28:29.717747-07:00",
                    "updated_at": "2025-08-25T15:30:46.181921-07:00",
                    "is_active": True,
                    "user": None,
                    "tracks_available": True,
                    "image_url": "/static/giraffe-male.svg",
                    "last_position_status": {
                        "last_voice_call_start_at": None,
                        "radio_state_at": None,
                        "radio_state": "na"
                    },
                    "last_position_date": "2025-08-23T15:01:02+00:00",
                    "last_position": {
                        "type": "Feature",
                        "geometry": {
                            "type": "Point",
                            "coordinates": [
                                -123.456,
                                47.123
                            ]
                        },
                        "properties": {
                            "title": "00001",
                            "subject_type": "wildlife",
                            "subject_subtype": "giraffe",
                            "id": "bc410952-86f0-4de6-93f6-e4b2c548872c",
                            "stroke": "#FFFF00",
                            "stroke-opacity": 1.0,
                            "stroke-width": 2,
                            "image": "https://gundi-dev.staging.pamdas.org/static/giraffe-male.svg",
                            "last_voice_call_start_at": None,
                            "location_requested_at": None,
                            "radio_state_at": "1970-01-01T00:00:00+00:00",
                            "radio_state": "na",
                            "coordinateProperties": {
                                "time": "2025-08-23T15:01:02+00:00"
                            },
                            "DateTime": "2025-08-23T15:01:02+00:00"
                        }
                    },
                    "device_status_properties": [
                        {
                            "label": "activity",
                            "units": None,
                            "value": 7
                        },
                        {
                            "label": "location_accuracy",
                            "units": None,
                            "value": 3
                        },
                        {
                            "label": "temperature",
                            "units": "C",
                            "value": 15
                        },
                        {
                            "label": "battery",
                            "units": "V",
                            "value": 32
                        }
                    ],
                    "url": "https://gundi-dev.staging.pamdas.org/api/v1.0/subject/bc410952-86f0-4de6-93f6-e4b2c548872c"
                },
                {
                    "content_type": "observations.subject",
                    "id": "f762afd6-ef2f-43fe-9b97-0aef2e7dc676",
                    "name": "Ranger X V1234",
                    "subject_type": "person",
                    "subject_subtype": "ranger",
                    "common_name": None,
                    "additional": {},
                    "created_at": "2025-08-25T08:16:58.040917-07:00",
                    "updated_at": "2025-08-25T08:16:58.040945-07:00",
                    "is_active": True,
                    "user": None,
                    "tracks_available": True,
                    "image_url": "/static/ranger-black.svg",
                    "last_position_status": {
                        "last_voice_call_start_at": None,
                        "radio_state_at": None,
                        "radio_state": "na"
                    },
                    "last_position_date": "2025-08-27T13:24:09+00:00",
                    "last_position": {
                        "type": "Feature",
                        "geometry": {
                            "type": "Point",
                            "coordinates": [
                                -123.456,
                                47.123
                            ]
                        },
                        "properties": {
                            "title": "Ranger X V1234",
                            "subject_type": "person",
                            "subject_subtype": "ranger",
                            "id": "f762afd6-ef2f-43fe-9b97-0aef2e7dc676",
                            "stroke": "#FFFF00",
                            "stroke-opacity": 1.0,
                            "stroke-width": 2,
                            "image": "https://gundi-dev.staging.pamdas.org/static/ranger-black.svg",
                            "last_voice_call_start_at": None,
                            "location_requested_at": None,
                            "radio_state_at": "1970-01-01T00:00:00+00:00",
                            "radio_state": "na",
                            "coordinateProperties": {
                                "time": "2025-08-27T13:24:09+00:00"
                            },
                            "DateTime": "2025-08-27T13:24:09+00:00"
                        }
                    },
                    "device_status_properties": None,
                    "url": "https://gundi-dev.staging.pamdas.org/api/v1.0/subject/f762afd6-ef2f-43fe-9b97-0aef2e7dc676"
                }
            ],
            "subgroups": []
        }
    ]

@pytest.fixture
def get_subjectgroups_flat_response():
    return [
        {
            "id": "07200222-9f6b-49ed-b390-c3c903948702",
            "name": "Telonics",
            "subjects": [

            ],
            "subgroups": []
        },
        {
            "id": "286fc226-095d-407f-9b17-b715761abdca",
            "name": "Stevens Connect",
            "subjects": [

            ],
            "subgroups": []
        },
        {
            "id": "28e5d22d-dc91-4160-9328-d5e5cd768ec6",
            "name": "Vital Weather",
            "subjects": [],
            "subgroups": []
        },
        {
            "id": "3ae98788-ee22-4283-bbd1-683872008f83",
            "name": "AWT",
            "subjects": [
                {
                    "content_type": "observations.subject",
                    "id": "bc410952-86f0-4de6-93f6-e4b2c548872c",
                    "name": "00001",
                    "subject_type": "wildlife",
                    "subject_subtype": "giraffe",
                    "common_name": "None",
                    "additional": {

                    },
                    "created_at": "2025-08-25T15:28:29.717747-07:00",
                    "updated_at": "2025-08-25T15:30:46.181921-07:00",
                    "is_active": True,
                    "user": "None",
                    "tracks_available": True,
                    "image_url": "/static/giraffe-male.svg",
                    "last_position_status": {
                        "last_voice_call_start_at": "None",
                        "radio_state_at": "None",
                        "radio_state": "na"
                    },
                    "last_position_date": "2025-08-23T15:01:02+00:00",
                    "last_position": {
                        "type": "Feature",
                        "geometry": {
                            "type": "Point",
                            "coordinates": [
                                -123.456,
                                47.123
                            ]
                        },
                        "properties": {
                            "title": "00001",
                            "subject_type": "wildlife",
                            "subject_subtype": "giraffe",
                            "id": "bc410952-86f0-4de6-93f6-e4b2c548872c",
                            "stroke": "#FFFF00",
                            "stroke-opacity": 1.0,
                            "stroke-width": 2,
                            "image": "https://gundi-dev.staging.pamdas.org/static/giraffe-male.svg",
                            "last_voice_call_start_at": "None",
                            "location_requested_at": "None",
                            "radio_state_at": "1970-01-01T00:00:00+00:00",
                            "radio_state": "na",
                            "coordinateProperties": {
                                "time": "2025-08-23T15:01:02+00:00"
                            },
                            "DateTime": "2025-08-23T15:01:02+00:00"
                        }
                    },
                    "device_status_properties": [
                        {
                            "label": "activity",
                            "units": "None",
                            "value": 7
                        },
                        {
                            "label": "location_accuracy",
                            "units": "None",
                            "value": 3
                        },
                        {
                            "label": "temperature",
                            "units": "C",
                            "value": 15
                        },
                        {
                            "label": "battery",
                            "units": "V",
                            "value": 32
                        }
                    ],
                    "url": "https://gundi-dev.staging.pamdas.org/api/v1.0/subject/bc410952-86f0-4de6-93f6-e4b2c548872c"
                }
            ],
            "subgroups": []
        },
        {
            "id": "a9672a01-c05a-4081-932f-4178c6124899",
            "name": "Seattle Radios",
            "subjects": [

            ],
            "subgroups": []
        },
        {
            "id": "b36ccafa-6651-4346-96b5-374b2b7b8166",
            "name": "DigitAnimal (Sintra Cascais Ambiente)",
            "subjects": [

            ],
            "subgroups": []
        },
        {
            "id": "bc8f7652-31e3-47fe-a185-060a18a23059",
            "name": "MMSubjectsWithSubGroup",
            "subjects": [

            ],
            "subgroups": []
        },
        {
            "id": "ca482c7a-f4c9-419a-9499-f107cc217c7c",
            "name": "Marine Monitor",
            "subjects": [

            ],
            "subgroups": []
        },
        {
            "id": "ccf85a2b-2e2e-4015-a2ff-135cf1e166c8",
            "name": "MMSubjectsSub",
            "subjects": [
                {
                    "content_type": "observations.subject",
                    "id": "51170be3-0ec1-4838-be07-ca9ad9954cbe",
                    "name": "MM Truck",
                    "subject_type": "unassigned",
                    "subject_subtype": "vehicle",
                    "common_name": "None",
                    "additional": {

                    },
                    "created_at": "2025-08-26T10:53:23.705391-07:00",
                    "updated_at": "2025-08-26T10:53:23.705428-07:00",
                    "is_active": True,
                    "user": "None",
                    "tracks_available": False,
                    "image_url": "/static/pin-black.svg",
                    "last_position_status": {
                        "last_voice_call_start_at": "None",
                        "radio_state_at": "None",
                        "radio_state": "na"
                    },
                    "device_status_properties": "None",
                    "url": "https://gundi-dev.staging.pamdas.org/api/v1.0/subject/51170be3-0ec1-4838-be07-ca9ad9954cbe"
                },
                {
                    "content_type": "observations.subject",
                    "id": "b9893e89-71c0-4d99-bff2-50e25eec2fe5",
                    "name": "Jeep X",
                    "subject_type": "unassigned",
                    "subject_subtype": "vehicle",
                    "common_name": "None",
                    "additional": {

                    },
                    "created_at": "2025-08-26T10:52:58.292713-07:00",
                    "updated_at": "2025-08-26T10:52:58.292740-07:00",
                    "is_active": True,
                    "user": "None",
                    "tracks_available": False,
                    "image_url": "/static/pin-black.svg",
                    "last_position_status": {
                        "last_voice_call_start_at": "None",
                        "radio_state_at": "None",
                        "radio_state": "na"
                    },
                    "device_status_properties": "None",
                    "url": "https://gundi-dev.staging.pamdas.org/api/v1.0/subject/b9893e89-71c0-4d99-bff2-50e25eec2fe5"
                }
            ],
            "subgroups": []
        },
        {
            "id": "f776fed7-0666-4146-a405-b2a0bd3ea274",
            "name": "MMSubjectsSubSub",
            "subjects": [
                {
                    "content_type": "observations.subject",
                    "id": "f8cb9fa2-55cb-4ef7-b2f6-2044d60c7606",
                    "name": "Mariano",
                    "subject_type": "person",
                    "subject_subtype": "ranger",
                    "common_name": "None",
                    "additional": {

                    },
                    "created_at": "2025-08-26T10:53:44.452504-07:00",
                    "updated_at": "2025-08-26T10:53:44.452534-07:00",
                    "is_active": True,
                    "user": "None",
                    "tracks_available": False,
                    "image_url": "/static/ranger-black.svg",
                    "last_position_status": {
                        "last_voice_call_start_at": "None",
                        "radio_state_at": "None",
                        "radio_state": "na"
                    },
                    "device_status_properties": "None",
                    "url": "https://gundi-dev.staging.pamdas.org/api/v1.0/subject/f8cb9fa2-55cb-4ef7-b2f6-2044d60c7606"
                }
            ],
            "subgroups": []
        },
        {
            "id": "ff039002-00af-48ad-acf4-218373991a07",
            "name": "Subjects",
            "subjects": [
                {
                    "content_type": "observations.subject",
                    "id": "f8cb9fa2-55cb-4ef7-b2f6-2044d60c7606",
                    "name": "Mariano",
                    "subject_type": "person",
                    "subject_subtype": "ranger",
                    "common_name": "None",
                    "additional": {

                    },
                    "created_at": "2025-08-26T10:53:44.452504-07:00",
                    "updated_at": "2025-08-26T10:53:44.452534-07:00",
                    "is_active": True,
                    "user": "None",
                    "tracks_available": False,
                    "image_url": "/static/ranger-black.svg",
                    "last_position_status": {
                        "last_voice_call_start_at": "None",
                        "radio_state_at": "None",
                        "radio_state": "na"
                    },
                    "device_status_properties": "None",
                    "url": "https://gundi-dev.staging.pamdas.org/api/v1.0/subject/f8cb9fa2-55cb-4ef7-b2f6-2044d60c7606"
                },
                {
                    "content_type": "observations.subject",
                    "id": "b9893e89-71c0-4d99-bff2-50e25eec2fe5",
                    "name": "Jeep X",
                    "subject_type": "unassigned",
                    "subject_subtype": "vehicle",
                    "common_name": "None",
                    "additional": {

                    },
                    "created_at": "2025-08-26T10:52:58.292713-07:00",
                    "updated_at": "2025-08-26T10:52:58.292740-07:00",
                    "is_active": True,
                    "user": "None",
                    "tracks_available": False,
                    "image_url": "/static/pin-black.svg",
                    "last_position_status": {
                        "last_voice_call_start_at": "None",
                        "radio_state_at": "None",
                        "radio_state": "na"
                    },
                    "device_status_properties": "None",
                    "url": "https://gundi-dev.staging.pamdas.org/api/v1.0/subject/b9893e89-71c0-4d99-bff2-50e25eec2fe5"
                },
                {
                    "content_type": "observations.subject",
                    "id": "bc410952-86f0-4de6-93f6-e4b2c548872c",
                    "name": "00001",
                    "subject_type": "wildlife",
                    "subject_subtype": "giraffe",
                    "common_name": "None",
                    "additional": {

                    },
                    "created_at": "2025-08-25T15:28:29.717747-07:00",
                    "updated_at": "2025-08-25T15:30:46.181921-07:00",
                    "is_active": True,
                    "user": "None",
                    "tracks_available": True,
                    "image_url": "/static/giraffe-male.svg",
                    "last_position_status": {
                        "last_voice_call_start_at": "None",
                        "radio_state_at": "None",
                        "radio_state": "na"
                    },
                    "last_position_date": "2025-08-23T15:01:02+00:00",
                    "last_position": {
                        "type": "Feature",
                        "geometry": {
                            "type": "Point",
                            "coordinates": [
                                -123.456,
                                47.123
                            ]
                        },
                        "properties": {
                            "title": "00001",
                            "subject_type": "wildlife",
                            "subject_subtype": "giraffe",
                            "id": "bc410952-86f0-4de6-93f6-e4b2c548872c",
                            "stroke": "#FFFF00",
                            "stroke-opacity": 1.0,
                            "stroke-width": 2,
                            "image": "https://gundi-dev.staging.pamdas.org/static/giraffe-male.svg",
                            "last_voice_call_start_at": "None",
                            "location_requested_at": "None",
                            "radio_state_at": "1970-01-01T00:00:00+00:00",
                            "radio_state": "na",
                            "coordinateProperties": {
                                "time": "2025-08-23T15:01:02+00:00"
                            },
                            "DateTime": "2025-08-23T15:01:02+00:00"
                        }
                    },
                    "device_status_properties": [
                        {
                            "label": "activity",
                            "units": "None",
                            "value": 7
                        },
                        {
                            "label": "location_accuracy",
                            "units": "None",
                            "value": 3
                        },
                        {
                            "label": "temperature",
                            "units": "C",
                            "value": 15
                        },
                        {
                            "label": "battery",
                            "units": "V",
                            "value": 32
                        }
                    ],
                    "url": "https://gundi-dev.staging.pamdas.org/api/v1.0/subject/bc410952-86f0-4de6-93f6-e4b2c548872c"
                },
                {
                    "content_type": "observations.subject",
                    "id": "f762afd6-ef2f-43fe-9b97-0aef2e7dc676",
                    "name": "Ranger X V1234",
                    "subject_type": "person",
                    "subject_subtype": "ranger",
                    "common_name": "None",
                    "additional": {

                    },
                    "created_at": "2025-08-25T08:16:58.040917-07:00",
                    "updated_at": "2025-08-25T08:16:58.040945-07:00",
                    "is_active": True,
                    "user": "None",
                    "tracks_available": True,
                    "image_url": "/static/ranger-black.svg",
                    "last_position_status": {
                        "last_voice_call_start_at": "None",
                        "radio_state_at": "None",
                        "radio_state": "na"
                    },
                    "last_position_date": "2025-08-27T20:34:09+00:00",
                    "last_position": {
                        "type": "Feature",
                        "geometry": {
                            "type": "Point",
                            "coordinates": [
                                -123.456,
                                47.123
                            ]
                        },
                        "properties": {
                            "title": "Ranger X V1234",
                            "subject_type": "person",
                            "subject_subtype": "ranger",
                            "id": "f762afd6-ef2f-43fe-9b97-0aef2e7dc676",
                            "stroke": "#FFFF00",
                            "stroke-opacity": 1.0,
                            "stroke-width": 2,
                            "image": "https://gundi-dev.staging.pamdas.org/static/ranger-black.svg",
                            "last_voice_call_start_at": "None",
                            "location_requested_at": "None",
                            "radio_state_at": "1970-01-01T00:00:00+00:00",
                            "radio_state": "na",
                            "coordinateProperties": {
                                "time": "2025-08-27T20:34:09+00:00"
                            },
                            "DateTime": "2025-08-27T20:34:09+00:00"
                        }
                    },
                    "device_status_properties": "None",
                    "url": "https://gundi-dev.staging.pamdas.org/api/v1.0/subject/f762afd6-ef2f-43fe-9b97-0aef2e7dc676"
                },
                {
                    "content_type": "observations.subject",
                    "id": "51170be3-0ec1-4838-be07-ca9ad9954cbe",
                    "name": "MM Truck",
                    "subject_type": "unassigned",
                    "subject_subtype": "vehicle",
                    "common_name": "None",
                    "additional": {

                    },
                    "created_at": "2025-08-26T10:53:23.705391-07:00",
                    "updated_at": "2025-08-26T10:53:23.705428-07:00",
                    "is_active": True,
                    "user": "None",
                    "tracks_available": False,
                    "image_url": "/static/pin-black.svg",
                    "last_position_status": {
                        "last_voice_call_start_at": "None",
                        "radio_state_at": "None",
                        "radio_state": "na"
                    },
                    "device_status_properties": "None",
                    "url": "https://gundi-dev.staging.pamdas.org/api/v1.0/subject/51170be3-0ec1-4838-be07-ca9ad9954cbe"
                }
            ],
            "subgroups": []
        }
    ]

@pytest.fixture
def expected_permissions_result_with_default_config():
    return {
        "User Details": {
            "Full Name": "Gundi Service Account",
            "Username": "gundi_serviceaccount",
            "Is Superuser": True
        },
        "Global Permissions": {
            "Event Category": [
                "add",
                "change",
                "delete",
                "view"
            ],
            "Event Type": [
                "add",
                "change",
                "delete",
                "view"
            ],
            "Event": [
                "add",
                "change",
                "delete",
                "export",
                "view"
            ],
            "Message": [
                "add",
                "change",
                "delete",
                "view"
            ],
            "Observation": [
                "export"
            ]
        },
        "Event Categories": {
            "Monitoring": {
                "Event Types": [
                    "Accident Report",
                    "Activity Alert",
                    "Ceres Tag Activity Alert",
                    "IAP Area",
                    "Mortality Alert",
                    "Mortality All Clear",
                    "Rainfall",
                    "Speed Alert",
                    "Survey 123 Response",
                    "Weather Station Summary",
                    "Wildlife Sighting",
                    "eBird Observations",
                    "iNaturalist",
                    "wpsWatch Integration"
                ],
                "Permissions": [
                    "add",
                    "change",
                    "delete",
                    "view"
                ]
            },
            "Wildlife Detection": {
                "Event Types": [
                    "Sentinel Event"
                ],
                "Permissions": [
                    "add",
                    "change",
                    "delete",
                    "view"
                ]
            },
            "Soilmentor Events": {
                "Event Types": [
                    "Soilmentor"
                ],
                "Permissions": [
                    "add",
                    "change",
                    "delete",
                    "view"
                ]
            },
            "Security": {
                "Event Types": [
                    "Camera Trap",
                    "Sit Rep",
                    "Vesel Detection SL"
                ],
                "Permissions": [
                    "add",
                    "change",
                    "delete",
                    "view"
                ]
            },
            "Analyzer Event": {
                "Event Types": [
                    "GFW Fire Alert",
                    "GFW Integrated Alerts",
                    "Ororatech Fire"
                ],
                "Permissions": [
                    "view"
                ]
            },
            "Everywhere Communications Events": {
                "Event Types": [
                    "Check in I'm OK",
                    "Check-in not OK",
                    "Emergency Entered",
                    "Emergency Exited",
                    "Geofence Entered",
                    "Geofence Exited",
                    "Missed Check-in",
                    "Missed Check-in Escalation",
                    "News Alert (FACTAL)",
                    "Self Check-in Started",
                    "Self Check-in Stopped"
                ],
                "Permissions": [
                    "add",
                    "change",
                    "delete",
                    "view"
                ]
            },
            "Skylight": {
                "Event Types": [
                    "Entry Alert"
                ],
                "Permissions": [
                    "add",
                    "change",
                    "delete",
                    "view"
                ]
            }
        },
        "Subject Groups": {
            "Telonics": [],
            "Stevens Connect": [],
            "Vital Weather": [],
            "AWT": [
                "00001"
            ],
            "Seattle Radios": [],
            "DigitAnimal (Sintra Cascais Ambiente)": [],
            "MMSubjectsWithSubGroup": [
                "Jeep X",
                "MM Truck",
                "Mariano"
            ],
            "MMSubjectsSub": [
                "Jeep X",
                "MM Truck",
                "Mariano"
            ],
            "MMSubjectsSubSub": [
                "Mariano"
            ],
            "Marine Monitor": [],
            "Subjects": [
                "00001",
                "Jeep X",
                "MM Truck",
                "Mariano",
                "Ranger X V1234"
            ]
        }
    }


@pytest.fixture
def expected_permissions_result_with_include_subjects_from_subgroups_false():
    return {
        "User Details": {
            "Full Name": "Gundi Service Account",
            "Username": "gundi_serviceaccount",
            "Is Superuser": True
        },
        "Global Permissions": {
            "Event Category": [
                "add",
                "change",
                "delete",
                "view"
            ],
            "Event Type": [
                "add",
                "change",
                "delete",
                "view"
            ],
            "Event": [
                "add",
                "change",
                "delete",
                "export",
                "view"
            ],
            "Message": [
                "add",
                "change",
                "delete",
                "view"
            ],
            "Observation": [
                "export"
            ]
        },
        "Event Categories": {
            "Monitoring": {
                "Event Types": [
                    "Accident Report",
                    "Activity Alert",
                    "Ceres Tag Activity Alert",
                    "IAP Area",
                    "Mortality Alert",
                    "Mortality All Clear",
                    "Rainfall",
                    "Speed Alert",
                    "Survey 123 Response",
                    "Weather Station Summary",
                    "Wildlife Sighting",
                    "eBird Observations",
                    "iNaturalist",
                    "wpsWatch Integration"
                ],
                "Permissions": [
                    "add",
                    "change",
                    "delete",
                    "view"
                ]
            },
            "Wildlife Detection": {
                "Event Types": [
                    "Sentinel Event"
                ],
                "Permissions": [
                    "add",
                    "change",
                    "delete",
                    "view"
                ]
            },
            "Soilmentor Events": {
                "Event Types": [
                    "Soilmentor"
                ],
                "Permissions": [
                    "add",
                    "change",
                    "delete",
                    "view"
                ]
            },
            "Security": {
                "Event Types": [
                    "Camera Trap",
                    "Sit Rep",
                    "Vesel Detection SL"
                ],
                "Permissions": [
                    "add",
                    "change",
                    "delete",
                    "view"
                ]
            },
            "Analyzer Event": {
                "Event Types": [
                    "GFW Fire Alert",
                    "GFW Integrated Alerts",
                    "Ororatech Fire"
                ],
                "Permissions": [
                    "view"
                ]
            },
            "Everywhere Communications Events": {
                "Event Types": [
                    "Check in I'm OK",
                    "Check-in not OK",
                    "Emergency Entered",
                    "Emergency Exited",
                    "Geofence Entered",
                    "Geofence Exited",
                    "Missed Check-in",
                    "Missed Check-in Escalation",
                    "News Alert (FACTAL)",
                    "Self Check-in Started",
                    "Self Check-in Stopped"
                ],
                "Permissions": [
                    "add",
                    "change",
                    "delete",
                    "view"
                ]
            },
            "Skylight": {
                "Event Types": [
                    "Entry Alert"
                ],
                "Permissions": [
                    "add",
                    "change",
                    "delete",
                    "view"
                ]
            }
        },
        "Subject Groups": {
            "Telonics": [],
            "Stevens Connect": [],
            "Vital Weather": [],
            "AWT": [
                "00001"
            ],
            "Seattle Radios": [],
            "DigitAnimal (Sintra Cascais Ambiente)": [],
            "MMSubjectsWithSubGroup": [],
            "MMSubjectsSub": [
                "Jeep X",
                "MM Truck",
            ],
            "MMSubjectsSubSub": [
                "Mariano"
            ],
            "Marine Monitor": [],
            "Subjects": [
                "00001",
                "Jeep X",
                "MM Truck",
                "Mariano",
                "Ranger X V1234"
            ]
        }
    }




class AsyncIterator:
    def __init__(self, seq):
        self.iter = iter(seq)

    def __aiter__(self):
        return self

    async def __anext__(self):
        try:
            return next(self.iter)
        except StopIteration:
            raise StopAsyncIteration


@pytest.fixture
def get_events_response(events_batch_one, events_batch_two):
    return [
        events_batch_one,
        events_batch_two
    ]


@pytest.fixture
def events_batch_one():
    return [
        {'id': 'a7a5e9ad-e157-4f95-a824-1d59dbb56c3d', 'location': None, 'time': '2023-11-17T14:14:34.480590-06:00',
         'end_time': None, 'serial_number': 433, 'message': '', 'provenance': '',
         'event_type': 'silence_source_provider_rep', 'priority': 0, 'priority_label': 'Gray', 'attributes': {},
         'comment': None, 'title': 'A7a9e1ab-44e2-4585-8d4f-7770ca0b36e2 integration disrupted', 'reported_by': None,
         'state': 'new', 'is_contained_in': [], 'sort_at': '2023-11-17T14:14:34.482529-06:00', 'patrol_segments': [],
         'geometry': None, 'updated_at': '2023-11-17T14:14:34.482529-06:00',
         'created_at': '2023-11-17T14:14:34.482823-06:00', 'icon_id': 'silence_source_provider_rep',
         'event_details': {'report_time': '2023-11-17 20:14:34', 'silence_threshold': '00:01',
                           'last_device_reported_at': '2023-10-23 00:44:32', 'updates': []}, 'files': [],
         'related_subjects': [], 'event_category': 'analyzer_event',
         'url': 'https://gundi-er.pamdas.org/api/v1.0/activity/event/a7a5e9ad-e157-4f95-a824-1d59dbb56c3d',
         'image_url': 'https://gundi-er.pamdas.org/static/generic-gray.svg', 'geojson': None, 'is_collection': False,
         'updates': [{'message': 'Created', 'time': '2023-11-17T20:14:34.524434+00:00',
                      'user': {'first_name': '', 'last_name': '', 'username': ''}, 'type': 'add_event'}],
         'patrols': []},
        {'id': '72448da2-8e80-48d3-81c4-fc6d86c275a8', 'location': None, 'time': '2023-11-17T13:14:35.122386-06:00',
         'end_time': None, 'serial_number': 432, 'message': '', 'provenance': '',
         'event_type': 'silence_source_provider_rep', 'priority': 0, 'priority_label': 'Gray', 'attributes': {},
         'comment': None, 'title': '265de4c0-07b8-4e30-b136-5d5a75ff5912 integration disrupted', 'reported_by': None,
         'state': 'new', 'is_contained_in': [], 'sort_at': '2023-11-17T13:14:35.124145-06:00', 'patrol_segments': [],
         'geometry': None, 'updated_at': '2023-11-17T13:14:35.124145-06:00',
         'created_at': '2023-11-17T13:14:35.124427-06:00', 'icon_id': 'silence_source_provider_rep',
         'event_details': {'report_time': '2023-11-17 19:14:34', 'silence_threshold': '00:00',
                           'last_device_reported_at': '2023-10-26 21:24:02', 'updates': []}, 'files': [],
         'related_subjects': [], 'event_category': 'analyzer_event',
         'url': 'https://gundi-er.pamdas.org/api/v1.0/activity/event/72448da2-8e80-48d3-81c4-fc6d86c275a8',
         'image_url': 'https://gundi-er.pamdas.org/static/generic-gray.svg', 'geojson': None, 'is_collection': False,
         'updates': [{'message': 'Created', 'time': '2023-11-17T19:14:35.130037+00:00',
                      'user': {'first_name': '', 'last_name': '', 'username': ''}, 'type': 'add_event'}],
         'patrols': []}
    ]


@pytest.fixture
def events_batch_two():
    return [
        {'id': '2d3a5877-475a-423e-97c5-5eead34010e2', 'location': None, 'time': '2023-11-17T13:14:34.481295-06:00',
         'end_time': None, 'serial_number': 431, 'message': '', 'provenance': '',
         'event_type': 'silence_source_provider_rep', 'priority': 0, 'priority_label': 'Gray', 'attributes': {},
         'comment': None, 'title': 'A7a9e1ab-44e2-4585-8d4f-7770ca0b36e2 integration disrupted', 'reported_by': None,
         'state': 'new', 'is_contained_in': [], 'sort_at': '2023-11-17T13:14:34.483293-06:00', 'patrol_segments': [],
         'geometry': None, 'updated_at': '2023-11-17T13:14:34.483293-06:00',
         'created_at': '2023-11-17T13:14:34.483539-06:00', 'icon_id': 'silence_source_provider_rep',
         'event_details': {'report_time': '2023-11-17 19:14:34', 'silence_threshold': '00:01',
                           'last_device_reported_at': '2023-10-23 00:44:32', 'updates': []}, 'files': [],
         'related_subjects': [], 'event_category': 'analyzer_event',
         'url': 'https://gundi-er.pamdas.org/api/v1.0/activity/event/2d3a5877-475a-423e-97c5-5eead34010e2',
         'image_url': 'https://gundi-er.pamdas.org/static/generic-gray.svg', 'geojson': None, 'is_collection': False,
         'updates': [{'message': 'Created', 'time': '2023-11-17T19:14:34.528430+00:00',
                      'user': {'first_name': '', 'last_name': '', 'username': ''}, 'type': 'add_event'}],
         'patrols': []},
        {'id': '950401f9-5a14-4dd7-be53-e90168c9474a', 'location': {'latitude': 39.963, 'longitude': -77.152},
         'time': '2023-11-17T12:29:43.477991-06:00', 'end_time': None, 'serial_number': 430, 'message': '',
         'provenance': '', 'event_type': 'trailguard_rep', 'priority': 200, 'priority_label': 'Amber', 'attributes': {},
         'comment': None, 'title': 'Trailguard Trap', 'reported_by': None, 'state': 'active', 'is_contained_in': [],
         'sort_at': '2023-11-17T12:30:08.818727-06:00', 'patrol_segments': [], 'geometry': None,
         'updated_at': '2023-11-17T12:30:08.818727-06:00', 'created_at': '2023-11-17T12:29:53.171274-06:00',
         'icon_id': 'cameratrap_rep',
         'event_details': {'labels': ['adult', 'poacher'], 'species': 'human', 'animal_count': 1, 'updates': []},
         'files': [{'id': '67888221-3e2a-4ba4-9cc7-ea0539f1e5f3', 'comment': '',
                    'created_at': '2023-11-17T12:30:08.802202-06:00', 'updated_at': '2023-11-17T12:30:08.802222-06:00',
                    'updates': [{'message': 'File Added: a7dcd2bc-703b-4324-bfd3-13ea130e7395_poacher.jpg',
                                 'time': '2023-11-17T18:30:08.810486+00:00', 'text': '',
                                 'user': {'username': 'gundi_serviceaccout', 'first_name': 'Gundi',
                                          'last_name': 'Service Account', 'id': 'ddc888bb-d642-455a-a422-7393b4f172be',
                                          'content_type': 'accounts.user'}, 'type': 'add_eventfile'}],
                    'url': 'https://gundi-er.pamdas.org/api/v1.0/activity/event/950401f9-5a14-4dd7-be53-e90168c9474a/file/67888221-3e2a-4ba4-9cc7-ea0539f1e5f3/',
                    'images': {
                        'original': 'https://gundi-er.pamdas.org/api/v1.0/activity/event/950401f9-5a14-4dd7-be53-e90168c9474a/file/67888221-3e2a-4ba4-9cc7-ea0539f1e5f3/original/a7dcd2bc-703b-4324-bfd3-13ea130e7395_poacher.jpg',
                        'icon': 'https://gundi-er.pamdas.org/api/v1.0/activity/event/950401f9-5a14-4dd7-be53-e90168c9474a/file/67888221-3e2a-4ba4-9cc7-ea0539f1e5f3/icon/a7dcd2bc-703b-4324-bfd3-13ea130e7395_poacher.jpg',
                        'thumbnail': 'https://gundi-er.pamdas.org/api/v1.0/activity/event/950401f9-5a14-4dd7-be53-e90168c9474a/file/67888221-3e2a-4ba4-9cc7-ea0539f1e5f3/thumbnail/a7dcd2bc-703b-4324-bfd3-13ea130e7395_poacher.jpg',
                        'large': 'https://gundi-er.pamdas.org/api/v1.0/activity/event/950401f9-5a14-4dd7-be53-e90168c9474a/file/67888221-3e2a-4ba4-9cc7-ea0539f1e5f3/large/a7dcd2bc-703b-4324-bfd3-13ea130e7395_poacher.jpg',
                        'xlarge': 'https://gundi-er.pamdas.org/api/v1.0/activity/event/950401f9-5a14-4dd7-be53-e90168c9474a/file/67888221-3e2a-4ba4-9cc7-ea0539f1e5f3/xlarge/a7dcd2bc-703b-4324-bfd3-13ea130e7395_poacher.jpg'},
                    'filename': 'a7dcd2bc-703b-4324-bfd3-13ea130e7395_poacher.jpg', 'file_type': 'image',
                    'icon_url': 'https://gundi-er.pamdas.org/api/v1.0/activity/event/950401f9-5a14-4dd7-be53-e90168c9474a/file/67888221-3e2a-4ba4-9cc7-ea0539f1e5f3/icon/a7dcd2bc-703b-4324-bfd3-13ea130e7395_poacher.jpg'}],
         'related_subjects': [], 'event_category': 'monitoring',
         'url': 'https://gundi-er.pamdas.org/api/v1.0/activity/event/950401f9-5a14-4dd7-be53-e90168c9474a',
         'image_url': 'https://gundi-er.pamdas.org/static/cameratrap-black.svg',
         'geojson': {'type': 'Feature', 'geometry': {'type': 'Point', 'coordinates': [-77.152, 39.963]},
                     'properties': {'message': '', 'datetime': '2023-11-17T18:29:43.477991+00:00',
                                    'image': 'https://gundi-er.pamdas.org/static/cameratrap-black.svg',
                                    'icon': {'iconUrl': 'https://gundi-er.pamdas.org/static/cameratrap-black.svg',
                                             'iconSize': [25, 25], 'iconAncor': [12, 12], 'popupAncor': [0, -13],
                                             'className': 'dot'}}}, 'is_collection': False, 'updates': [
            {'message': 'Changed State: new → active', 'time': '2023-11-17T18:30:08.831311+00:00',
             'user': {'username': 'gundi_serviceaccout', 'first_name': 'Gundi', 'last_name': 'Service Account',
                      'id': 'ddc888bb-d642-455a-a422-7393b4f172be', 'content_type': 'accounts.user'}, 'type': 'read'},
            {'message': 'File Added: a7dcd2bc-703b-4324-bfd3-13ea130e7395_poacher.jpg',
             'time': '2023-11-17T18:30:08.810486+00:00', 'text': '',
             'user': {'username': 'gundi_serviceaccout', 'first_name': 'Gundi', 'last_name': 'Service Account',
                      'id': 'ddc888bb-d642-455a-a422-7393b4f172be', 'content_type': 'accounts.user'},
             'type': 'add_eventfile'}, {'message': 'Created', 'time': '2023-11-17T18:29:53.207751+00:00',
                                        'user': {'username': 'gundi_serviceaccout', 'first_name': 'Gundi',
                                                 'last_name': 'Service Account',
                                                 'id': 'ddc888bb-d642-455a-a422-7393b4f172be',
                                                 'content_type': 'accounts.user'}, 'type': 'add_event'}], 'patrols': []}
    ]


@pytest.fixture
def get_observations_response(observations_batch_one, observations_batch_two):
    return [
        observations_batch_one,
        observations_batch_two
    ]


@pytest.fixture
def observations_batch_one():
    return [
        {'id': 'f4a4d03f-4da6-45ec-a330-cd9c0e74f3fb', 'location': {'longitude': 36.7911946, 'latitude': -1.2921562},
         'recorded_at': '2023-11-10T05:34:47+00:00', 'created_at': '2023-11-10T05:34:49+00:00', 'exclusion_flags': 0,
         'source': '192b457f-fa25-4674-ae3e-8fae8d775d61', 'observation_details': {}},
        {'id': 'b0a81cc9-22ab-46fe-875b-b7c1e58071c4', 'location': {'longitude': 36.790968, 'latitude': -1.2921935},
         'recorded_at': '2023-11-10T05:35:08+00:00', 'created_at': '2023-11-10T05:35:09+00:00', 'exclusion_flags': 0,
         'source': '192b457f-fa25-4674-ae3e-8fae8d775d61', 'observation_details': {}}
    ]


@pytest.fixture
def observations_batch_two():
    return [
        {'id': '8dc59651-3784-4e30-9d07-a54057fd0f4a', 'location': {'longitude': 36.7907403, 'latitude': -1.2921814},
         'recorded_at': '2023-11-10T05:35:30+00:00', 'created_at': '2023-11-10T05:35:30+00:00', 'exclusion_flags': 0,
         'source': '192b457f-fa25-4674-ae3e-8fae8d775d61', 'observation_details': {}},
        {'id': '6b56787b-6a40-462f-9869-1f42a7909a1a', 'location': {'longitude': 36.7906881, 'latitude': -1.2919601},
         'recorded_at': '2023-11-10T05:35:48+00:00', 'created_at': '2023-11-10T05:35:49+00:00', 'exclusion_flags': 0,
         'source': '192b457f-fa25-4674-ae3e-8fae8d775d61', 'observation_details': {}}
    ]


@pytest.fixture
def mock_get_gundi_api_key(mocker, mock_api_key):
    mock = mocker.MagicMock()
    mock.return_value = async_return(mock_api_key)
    return mock


@pytest.fixture
def mock_api_key():
    return "MockAP1K3y"


@pytest.fixture
def er_client_close_response():
    return {}


@pytest.fixture
def mock_config_manager_er_provider(mocker, er_integration_v2_provider):

    async def mock_get_action_configuration(integration_id, action_id):
        return er_integration_v2_provider.get_action_config(action_id)

    mock_config_manager_er = mocker.MagicMock()
    mock_config_manager_er.get_integration.return_value = async_return(
        IntegrationSummary.from_integration(er_integration_v2_provider)
    )
    mock_config_manager_er.get_integration_details.return_value = async_return(er_integration_v2_provider)
    mock_config_manager_er.get_action_configuration.side_effect = mock_get_action_configuration
    mock_config_manager_er.set_integration.return_value = async_return(None)
    mock_config_manager_er.set_action_configuration.return_value = async_return(None)
    mock_config_manager_er.delete_integration.return_value = async_return(None)
    mock_config_manager_er.delete_action_configuration.return_value = async_return(None)
    return mock_config_manager_er


@pytest.fixture
def mock_config_manager_er_destination(mocker, er_integration_v2_destination):

    async def mock_get_action_configuration(integration_id, action_id):
        return er_integration_v2_destination.get_action_config(action_id)

    mock_config_manager_er = mocker.MagicMock()
    mock_config_manager_er.get_integration.return_value = async_return(
        IntegrationSummary.from_integration(er_integration_v2_destination)
    )
    mock_config_manager_er.get_integration_details.return_value = async_return(er_integration_v2_destination)
    mock_config_manager_er.get_action_configuration.side_effect = mock_get_action_configuration
    mock_config_manager_er.set_integration.return_value = async_return(None)
    mock_config_manager_er.set_action_configuration.return_value = async_return(None)
    mock_config_manager_er.delete_integration.return_value = async_return(None)
    mock_config_manager_er.delete_action_configuration.return_value = async_return(None)
    return mock_config_manager_er
