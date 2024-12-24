import json
import datetime
import logging
from urllib.parse import urlparse

import httpx
import stamina
from erclient import AsyncERClient, ERClientException
from gundi_client_v2.client import GundiClient
from gundi_core.schemas.v2 import Integration
from app.services.utils import find_config_for_action
from app.services.state import IntegrationStateManager
from .configurations import AuthenticateConfig, PullObservationsConfig, PullEventsConfig
from ..services.activity_logger import activity_logger
from ..services.gundi import send_events_to_gundi, send_observations_to_gundi

logger = logging.getLogger(__name__)


DEFAULT_CONNECT_TIMEOUT_SECONDS = 10.0
BATCH_SIZE = 100
state_manager = IntegrationStateManager()


async def action_auth(integration: Integration, action_config: AuthenticateConfig):
    auth_config = action_config
    url_parse = urlparse(integration.base_url)
    async with AsyncERClient(
            service_root=f"{url_parse.scheme}://{url_parse.hostname}/api/v1.0",
            username=auth_config.username,
            password=auth_config.password.get_secret_value() if auth_config.password else None,
            token=auth_config.token,
            token_url=f"{url_parse.scheme}://{url_parse.hostname}/oauth2/token",
            client_id="das_web_client",
            connect_timeout=DEFAULT_CONNECT_TIMEOUT_SECONDS,
    ) as er_client:
        try:
            if auth_config.token:
                result = await er_client.get_me()
                # ToDo: Support doing a deeper check on permissions here or in a separate handler
                valid_credentials = result.get('is_active', False)
            elif auth_config.username and auth_config.password:
                valid_credentials = await er_client.login()
            else:
                return {"valid_credentials": False, "error": "Please provide either a token or username/password."}
        except ERClientException as e:
            # ToDo. Differentiate ER errors from invalid credentials in the ER client
            return {"valid_credentials": False, "error": str(e)}
        return {"valid_credentials": valid_credentials}


@activity_logger()
async def action_pull_events(integration: Integration, action_config: PullEventsConfig):
    integration_id = str(integration.id)
    logger.info(
        f"Extracting events for integration {integration_id}, with config {action_config}",
    )
    total_events = 0
    execution_timestamp = datetime.datetime.now(tz=datetime.timezone.utc).isoformat()
    # Parse configurations
    pull_config = action_config
    auth_config = get_authentication_config(integration=integration)
    # Prepare the ER client to extract events
    url_parse = urlparse(integration.base_url)
    er_client = AsyncERClient(
        service_root=f"{url_parse.scheme}://{url_parse.hostname}/api/v1.0",
        username=auth_config.username or None,
        password=auth_config.password.get_secret_value() if auth_config.password else None,
        token=auth_config.token,
        token_url=f"{url_parse.scheme}://{url_parse.hostname}/oauth2/token",
        client_id="das_web_client",
        connect_timeout=DEFAULT_CONNECT_TIMEOUT_SECONDS,
    )
    # Prepare filters to extract Data Since last execution
    state = await state_manager.get_state(
        integration_id=integration_id, action_id="pull_events"
    )
    logger.debug(
        f"State retrieved for integration {integration_id}, action pull_events:\n{state}",
    )
    last_execution = state.get("last_execution")
    if not last_execution or pull_config.force_run_since_start:
        start_datetime = pull_config.start_datetime
    else:
        start_datetime = last_execution
    filter = {
        "date_range": {
            "lower": start_datetime
        }
    }
    if pull_config.end_datetime:
        filter["date_range"]["upper"] = pull_config.end_datetime
    json_filter = json.dumps(filter)
    logger.info(f"Extracting events with filter '{filter}'...")
    # Process events in batches
    async with er_client as earth_ranger:
        async for event_batch in earth_ranger.get_events(filter=json_filter, batch_size=BATCH_SIZE):
            transformed_events = transform_events_to_gundi_schema(events=event_batch)
            if not transformed_events:
                continue  # No data to send
            logger.info(f"Sending {len(transformed_events)} events to Gundi...")
            await send_events_to_gundi(events=transformed_events, integration_id=integration_id)
            total_events += len(transformed_events)
    # Update state
    state = {"last_execution": execution_timestamp}
    logger.debug(f"Saving state for integration {integration}, action pull_events:\n{state}")
    await state_manager.set_state(
        integration_id=integration_id,
        action_id="pull_events",
        state=state
    )
    logger.info(f"Extracted {total_events} events.")
    return {"events_extracted": total_events}


@activity_logger()
async def action_pull_observations(integration: Integration, action_config: PullObservationsConfig):
    integration_id = str(integration.id)
    logger.info(
        f"Extracting observations for integration {integration_id}, with config {action_config}",
    )
    total_observations = 0
    execution_timestamp = datetime.datetime.now(tz=datetime.timezone.utc).isoformat()
    # Parse configurations
    pull_config = action_config
    auth_config = get_authentication_config(integration=integration)
    # Prepare the ER client to extract events
    url_parse = urlparse(integration.base_url)
    er_client = AsyncERClient(
        service_root=f"{url_parse.scheme}://{url_parse.hostname}/api/v1.0",
        username=auth_config.username or None,
        password=auth_config.password.get_secret_value() if auth_config.password else None,
        token=auth_config.token,
        token_url=f"{url_parse.scheme}://{url_parse.hostname}/oauth2/token",
        client_id="das_web_client",
        connect_timeout=DEFAULT_CONNECT_TIMEOUT_SECONDS,
    )
    # Prepare filters to extract Data Since last execution
    state = await state_manager.get_state(
        integration_id=integration_id, action_id="pull_observations"
    )
    logger.debug(
        f"State retrieved for integration {integration_id}, action pull_observations:\n{state}"
    )
    last_execution = state.get("last_execution")
    if not last_execution or pull_config.force_run_since_start:
        start_datetime = pull_config.start_datetime
    else:
        start_datetime = last_execution
    params = {
        "start": start_datetime,
        "batch_size": BATCH_SIZE
    }
    if pull_config.end_datetime:
        params["end"] = pull_config.end_datetime
    # Process events in batches
    logger.info(f"Extracting observations with params '{params}'...")
    async with er_client as earth_ranger:
        async for observation_batch in earth_ranger.get_observations(**params):
            transformed_observations = transform_observations_to_gundi_schema(observations=observation_batch)
            if not transformed_observations:
                continue  # No data to send
            logger.info(f"Sending {len(transformed_observations)} observations to Gundi...")
            await send_observations_to_gundi(observations=transformed_observations, integration_id=integration_id)
            total_observations += len(transformed_observations)
    # Update state
    state = {"last_execution": execution_timestamp}
    logger.debug(f"Saving state for integration {integration}, action pull_observations:\n{state}")
    await state_manager.set_state(
        integration_id=integration_id,
        action_id="pull_observations",
        state=state
    )
    logger.info(f"Extracted {total_observations} observations for integration {integration}.")
    return {"observations_extracted": total_observations}


# Auxiliary functions

def get_authentication_config(integration):
    configurations = integration.configurations
    auth_action_config = find_config_for_action(
        configurations=configurations,
        action_id='auth'
    )
    if not auth_action_config:
        raise ValueError(
            f"Authentication settings for integration {str(integration.id)} are missing. Please fix the integration setup in the portal."
        )
    return AuthenticateConfig.parse_obj(auth_action_config.data)


def transform_events_to_gundi_schema(events):
    transformed_data = []
    for event in events:
        try:
            transformed_event = {}
            # Set base fields
            if event_type := event.get("event_type"):
                transformed_event["event_type"] = event_type
            if title := event.get("title"):
                transformed_event["title"] = title
            if recorded_at := event.get("created_at"):
                transformed_event["recorded_at"] = recorded_at
            if geometry := event.get("geojson"):
                transformed_event["geometry"] = geometry
            if event_details := event.get("event_details"):
                transformed_event["event_details"] = event_details
            if location := event.get("location"):
                transformed_event["location"] = {
                    "lon": location.get("longitude"),
                    "lat": location.get("latitude")
                }
            # Save others fields in additional
            transformed_event["additional"] = {
                key: value for key, value in event.items()
                if key not in transformed_event.keys()
            }
        except Exception as e:
            logger.error(
                f"Error transforming event {event}: {e}",
                extra={"attention_needed": True}
            )
            continue
        else:
            transformed_data.append(transformed_event)
    return transformed_data


def transform_observations_to_gundi_schema(observations):
    transformed_data = []
    for observation in observations:
        try:
            transformed_observation = {}
            # Set base fields
            if recorded_at := observation.get("recorded_at"):
                transformed_observation["recorded_at"] = recorded_at
            if source := observation.get("source"):
                transformed_observation["source"] = f"er-src-{source}"
            if location := observation.get("location"):
                transformed_observation["location"] = {
                    "lon": location.get("longitude"),
                    "lat": location.get("latitude")
                }
            # Save others fields in additional
            transformed_observation["additional"] = {
                key: value for key, value in observation.items()
                if key not in transformed_observation.keys()
            }
        except Exception as e:
            logger.error(
                f"Error transforming observation {observation}: {e}",
                extra={"attention_needed": True}
            )
            continue
        else:
            transformed_data.append(transformed_observation)
    return transformed_data

