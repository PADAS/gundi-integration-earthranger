import asyncio

import httpx
import pytest
from erclient import ERClientException
from gundi_core.schemas.v2 import Integration, IntegrationSummary


def async_return(result):
    f = asyncio.Future()
    f.set_result(result)
    return f


@pytest.fixture
def er_integration_v2():
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
        er_integration_v2,
):
    mock_client = mocker.MagicMock()
    mock_client.get_integration_details.return_value = async_return(
        er_integration_v2
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
        get_events_response,
        get_observations_response,
        er_client_close_response
):
    mocked_erclient_class = mocker.MagicMock()
    erclient_mock = mocker.MagicMock()
    erclient_mock.get_me.return_value = async_return(
        get_me_response
    )
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
def er_401_exception():
    return ERClientException(
        'Failed to GET to ER web service. provider_key: None, service: https://gundi-dev.staging.pamdas.org/api/v1.0, path: user/me,\n\t 401 from ER. Message: Authentication credentials were not provided. {"status":{"code":401,"message":"Unauthorized","detail":"Authentication credentials were not provided."}}'
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
def mock_erclient_class_with_auth_401(
        mocker,
        auth_headers_response,
        er_401_exception,

):
    mocked_erclient_class = mocker.MagicMock()
    erclient_mock = mocker.MagicMock()
    erclient_mock.get_me.side_effect = er_401_exception
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
        'username': 'gundi_serviceaccount',
        'email': None,
        'first_name': 'Gundi',
        'last_name': 'Service Account',
        'role': '',
        'is_staff': False,
        'is_superuser': True,
        'date_joined': '2022-08-31T03:21:05.891041-07:00',
        'id': 'ebd8ef0f-8b86-4e0f-8b34-b55a575e476f',
        'is_active': True,
        'last_login': '2024-08-28T15:16:55.894706-07:00',
        'pin': None,
        'subject': None,
        'permissions': {
            'wildlife_resourceuse': ['view', 'delete', 'add', 'change'],
            'patrolconfiguration': ['view', 'change', 'delete', 'add'],
            'uwa_hdq': ['view', 'add', 'change', 'delete'],
            'eventclassfactor': ['delete', 'view', 'add', 'change'],
            'kvca_eland_monitoring_-2023': ['delete', 'change', 'add', 'view'],
            'ilegal-activities': ['delete', 'add', 'change', 'view'],
            'karamagi_patrol2': ['change', 'add', 'delete', 'view'],
            'eventdetails': ['view', 'delete', 'change', 'add'], 'monitoring': ['change', 'view', 'delete', 'add'],
            'community': ['view', 'view', 'delete', 'add', 'change', 'delete', 'change', 'add'],
            'wildlife_rangers': ['delete', 'add', 'view', 'change'], 'meetings': ['change', 'view', 'add', 'delete'],
            'murchison_falls_cc_unit': ['change', 'delete', 'add', 'view'],
            'patroltype': ['view', 'add', 'change', 'delete'], 'human_injury_mfca': ['delete', 'view', 'change', 'add'],
            'noaa_edgtech_geo': ['delete', 'change', 'view', 'add'],
            'problem-animal': ['delete', 'view', 'change', 'add'],
            'kvca_monitoring_eland': ['delete', 'change', 'add', 'view'], 'patrol': ['view', 'add', 'change', 'delete'],
            'hwc_puwr': ['change', 'delete', 'view', 'add'], 'zoologico': ['delete', 'view', 'add', 'change'],
            'crop_raid_rep': ['view', 'change', 'add', 'delete'], 'teammembership': ['view', 'delete', 'add', 'change'],
            'human_injury-1234': ['delete', 'change', 'add', 'view'],
            'hwc_monitoring': ['add', 'delete', 'change', 'view'],
            'vulture_clusters': ['add', 'delete', 'view', 'change'],
            'noaa_ashored_events': ['add', 'delete', 'change', 'view'],
            'patrol-teams': ['delete', 'change', 'view', 'add'], 'message': ['view', 'change', 'delete', 'add'],
            'eventfactor': ['view', 'change', 'delete', 'add'], 'eventcategory': ['add', 'view', 'change', 'delete'],
            'apitests': ['view', 'add', 'change', 'delete'], 'eventclass': ['add', 'view', 'delete', 'change'],
            'noaa_gearmanufacturer_events': ['change', 'view', 'delete', 'add'],
            'hwc_monitoring-bmca': ['add', 'change', 'view', 'delete'],
            'noaa_devocean_events': ['delete', 'add', 'change', 'view'],
            'tsvectormodel': ['view', 'change', 'delete', 'add'], 'smart_reports': ['add', 'view', 'change', 'delete'],
            'eventfilter': ['view', 'change', 'add', 'delete'], 'standard__deprecated': ['view', 'change'],
            'eventgeometry': ['delete', 'view', 'add', 'change'], 'hwc_incidences': ['add', 'view', 'change', 'delete'],
            'easterisland_monitoring': ['add', 'view', 'delete', 'change'],
            'patrol-3': ['delete', 'add', 'change', 'view'],
            'patrolconfigurationsubjectgroup': ['change', 'add', 'view', 'delete'],
            'bmca_training': ['change', 'view', 'delete', 'add'], 'security': ['change', 'view', 'add', 'delete'],
            'crop_raid_lmca1': ['delete', 'add', 'change', 'view'],
            'eventrelatedsubject': ['delete', 'change', 'view', 'add'],
            'patrolmen1': ['view', 'add', 'delete', 'change'], 'analyzer_event': ['change', 'delete', 'view', 'add'],
            'refreshrecreateeventdetailview': ['change', 'delete', 'view', 'add'],
            'security__deprecated': ['view', 'change'], 'kzn_vulturemonitoring': ['view', 'add', 'delete', 'change'],
            'sfg_rnd': ['change', 'delete', 'view', 'add'], 'qeca_monitoring_team': ['view', 'delete', 'change', 'add'],
            'eventrelatedsegments': ['delete', 'add', 'change', 'view'],
            'vcc_vcc_vulture_clusters': ['change', 'add', 'delete', 'view'],
            'alertrule': ['change', 'delete', 'view', 'add'],
            'shift_rangers_staff': ['view', 'change', 'add', 'delete'],
            'community-conservation': ['delete', 'view', 'change', 'add'],
            'eventsourceevent': ['delete', 'change', 'view', 'add'],
            'wildlife_reserve': ['delete', 'change', 'add', 'view'],
            'communty_rangers': ['change', 'delete', 'add', 'view'],
            'uwa-hdqs-team': ['add', 'view', 'change', 'delete'],
            'baotree_reports': ['change', 'add', 'delete', 'view'], 'test_cat': ['add', 'view', 'change', 'delete'],
            'guido': ['delete', 'add', 'view', 'change'], 'animal_census': ['add', 'change', 'delete', 'view'],
            'eventrelationship': ['view', 'change', 'delete', 'add'], 'eventphoto': ['add', 'view', 'change', 'delete'],
            'alertruleeventtype': ['add', 'view', 'delete', 'change'],
            'eventnotification': ['add', 'view', 'delete', 'change'], 'ecology': ['view', 'change', 'add', 'delete'],
            'wildlife_scouts': ['add', 'delete', 'view', 'change'], 'eventsource': ['view', 'change', 'add', 'delete'],
            'person': ['delete', 'add', 'view', 'change'], 'easterisland_security': ['change', 'delete', 'view', 'add'],
            'extended-patrols': ['delete', 'view', 'change', 'add'],
            'noaa_smelts_geo': ['view', 'delete', 'change', 'add'], 'eventfile': ['view', 'delete', 'change', 'add'],
            'easterisland_analyzer_event': ['change', 'delete', 'add', 'view'],
            'invasive_monitoring': ['change', 'delete', 'add', 'view'],
            'livestock_predation': ['add', 'change', 'view', 'delete'],
            'kasese_monitoring_crop_raid': ['view', 'delete', 'change', 'add'],
            'monitoring_and_research': ['add', 'view', 'change', 'delete'],
            'queen_elizabeth_np': ['view', 'add', 'delete', 'change'],
            'wildlife_resourceuse-1': ['add', 'delete', 'view', 'change'],
            'research_lmca': ['view', 'change', 'add', 'delete'], 'kvca_uwa': ['view', 'add', 'change', 'delete'],
            'law-enforcement': ['change', 'delete', 'view', 'add'], 'hwc': ['add', 'change', 'delete', 'view'],
            'vcc_vulture_clusters': ['view', 'change', 'add', 'delete'],
            'event': ['export', 'add', 'change', 'view', 'delete'],
            'easterisland_logistics': ['view', 'change', 'add', 'delete'],
            'logistics': ['add', 'view', 'delete', 'change'], 'uwrti_scouts': ['delete', 'add', 'view', 'change'],
            'patrol_men': ['delete', 'view', 'add', 'change'], 'animal_disposal2': ['change', 'add', 'delete', 'view'],
            'hidden': ['add', 'view', 'delete', 'change'], 'membershiptype': ['view', 'change', 'delete', 'add'],
            'problem_animal': ['delete', 'change', 'add', 'view'],
            'human_injury_rep': ['add', 'delete', 'view', 'change'], 'rangers': ['add', 'change', 'delete', 'view'],
            'mount_elgon_conservation_area': ['change', 'view', 'delete', 'add'],
            'ougen': ['view', 'delete', 'add', 'change'], 'hwc_allan': ['view', 'delete', 'change', 'add'],
            'animal_details': ['delete', 'view', 'change', 'add'],
            'monitoring_research': ['delete', 'change', 'add', 'view'],
            'qeca_community_conservation': ['view', 'delete', 'change', 'add'],
            'string': ['delete', 'change', 'add', 'view'], 'kabibi-general': ['view', 'change', 'add', 'delete'],
            'mornitoring': ['delete', 'change', 'add', 'view'], 'eventnote': ['add', 'change', 'view', 'delete'],
            'kakamega_test': ['change', 'delete', 'add', 'view'], 'emr-staff-qeca': ['add', 'change', 'view', 'delete'],
            'human_wildlif_conflict': ['view', 'change', 'add', 'delete'],
            'notificationmethod': ['add', 'delete', 'view', 'change'], 'eventtype': ['change', 'view', 'delete', 'add'],
            'team': ['add', 'delete', 'view', 'change'], 'eventattachment': ['delete', 'view', 'change', 'add'],
            'noaa_bog_geo': ['view', 'change', 'add', 'delete'],
            'hwc_monitoring_g5': ['delete', 'view', 'add', 'change'],
            'wildlife-monitoring': ['delete', 'add', 'view', 'change'],
            'community_conservations': ['delete', 'change', 'add', 'view'],
            'accident_couse': ['view', 'change', 'add', 'delete'],
            'eventrelationshiptype': ['delete', 'change', 'view', 'add'],
            'alertrulenotificationmethod': ['add', 'change', 'delete', 'view'],
            'trails': ['change', 'add', 'delete', 'view'], 'hwc-mitigation': ['view', 'change', 'delete', 'add'],
            'monitoring-puwr': ['delete', 'view', 'add', 'change'], 'hwc-mfca': ['add', 'view', 'delete', 'change'],
            'everywhere_comms': ['add', 'view', 'delete', 'change'],
            'easterisland_demo': ['change', 'add', 'view', 'delete'], 'ecology2': ['change', 'delete', 'add', 'view'],
            'mgnp_hugo': ['delete', 'view', 'change', 'add'], 'crop_raid_adriano': ['change', 'view', 'add', 'delete'],
            'kvca_hwc': ['delete', 'add', 'view', 'change'], 'observation': ['export'],
            'event_for_eventsource': ['add'], 'angiswa_staffs': ['add', 'change', 'view', 'delete'],
            'team_work': ['change', 'view', 'delete', 'add'], 'crop_raid_lmca': ['delete', 'change', 'add', 'view'],
            'group_two': ['add', 'delete', 'change', 'view'], 'hwc_-mfca': ['view', 'delete', 'add', 'change'],
            'mburo_staff': ['add', 'view', 'change', 'delete'],
            'community_conservation': ['change', 'add', 'delete', 'view'], 'meca': ['add', 'delete', 'change', 'view'],
            'mirembe_lmca': ['delete', 'change', 'add', 'view'], 'eventprovider': ['delete', 'view', 'add', 'change'],
            'kvca_pangolin_monitoring': ['delete', 'view', 'add', 'change'],
            'ropelessgear': ['change', 'delete', 'view', 'add'], 'karamagi-patrol': ['delete', 'view', 'change', 'add'],
            'lion_monitoring': ['view', 'delete', 'add', 'change'], 'porters': ['change', 'add', 'delete', 'view'],
            'hotleerymes': ['view', 'delete', 'change', 'add']
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
def mock_config_manager_er(mocker, er_integration_v2):

    async def mock_get_action_configuration(integration_id, action_id):
        return er_integration_v2.get_action_config(action_id)

    mock_config_manager_er = mocker.MagicMock()
    mock_config_manager_er.get_integration.return_value = async_return(
        IntegrationSummary.from_integration(er_integration_v2)
    )
    mock_config_manager_er.get_integration_details.return_value = async_return(er_integration_v2)
    mock_config_manager_er.get_action_configuration.side_effect = mock_get_action_configuration
    mock_config_manager_er.set_integration.return_value = async_return(None)
    mock_config_manager_er.set_action_configuration.return_value = async_return(None)
    mock_config_manager_er.delete_integration.return_value = async_return(None)
    mock_config_manager_er.delete_action_configuration.return_value = async_return(None)
    return mock_config_manager_er
