import json
import datetime
import logging
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Dict
from urllib.parse import urlparse

import httpx
import stamina
from erclient import AsyncERClient, ERClientException, VERSION_1_0, VERSION_2_0
from erclient.er_errors import ERClientBadCredentials
from gundi_client_v2.client import GundiClient
from gundi_core.events import LogLevel
from gundi_core.schemas.v2 import Integration
from app.services.utils import find_config_for_action
from app.services.state import IntegrationStateManager
from .configurations import AuthenticateConfig, EventFilterDateField, PullObservationsConfig, PullEventsConfig, \
    ERAuthenticationType, ShowPermissionsConfig
from ..services.activity_logger import activity_logger, log_action_activity
from ..services.gundi import send_events_to_gundi, send_observations_to_gundi, update_event_in_gundi

logger = logging.getLogger(__name__)


DEFAULT_CONNECT_TIMEOUT_SECONDS = 10.0
BATCH_SIZE = 100
state_manager = IntegrationStateManager()

# Maps the operator-selected date field to the corresponding key on ER's
# event-filter blob. ER applies these independently of each other, so only
# one is set per pull.
ER_EVENT_FILTER_KEY_BY_DATE_FIELD = {
    EventFilterDateField.EVENT_TIME: "date_range",
    EventFilterDateField.CREATED_AT: "create_date",
    EventFilterDateField.UPDATED_AT: "update_date",
}


async def action_auth(integration: Integration, action_config: AuthenticateConfig):
    auth_config = action_config
    url_parse = urlparse(integration.base_url)
    if not url_parse.hostname:
        return {"valid_credentials": False, "error": f"Site URL is empty or invalid: '{integration.base_url}'"}
    async with AsyncERClient(
            service_root=f"{url_parse.scheme}://{url_parse.hostname}/api/v1.0",
            username=auth_config.username,
            password=auth_config.password.get_secret_value() if auth_config.password else None,
            token=auth_config.token.get_secret_value() if auth_config.token else None,
            token_url=f"{url_parse.scheme}://{url_parse.hostname}/oauth2/token",
            client_id="das_web_client",
            connect_timeout=DEFAULT_CONNECT_TIMEOUT_SECONDS,
    ) as er_client:
        try:
            if auth_config.authentication_type == ERAuthenticationType.TOKEN:
                if not auth_config.token:
                    return {"valid_credentials": False, "error": "Please provide a token."}
                result = await er_client.get_me()
                # ToDo: Support doing a deeper check on permissions here or in a separate handler
                valid_credentials = result.get('is_active', False)
            elif auth_config.authentication_type == ERAuthenticationType.USERNAME_PASSWORD:
                if not auth_config.username or not auth_config.password:
                    return {"valid_credentials": False, "error": "Please provide both a username and a password."}
                valid_credentials = await er_client.login()
            else:
                return {"valid_credentials": False, "error": "Please select an valid authentication method."}
        except ERClientBadCredentials:
            return {"valid_credentials": False, "error": "Invalid credentials"}
        except ERClientException as e:
            # ToDo. Differentiate ER errors from invalid credentials in the ER client
            return {"valid_credentials": False, "error": str(e)}
        except httpx.HTTPError as e:
            return {"valid_credentials": False, "error": f"HTTP error: {e}"}
        return {"valid_credentials": valid_credentials}


def _extract_user_details(er_user_details):
    """
    ER API sample user details:
        {
            "username": "gundi_service_account",
            "email": null,
            "first_name": "Gundi Service Account",
            "last_name": "",
            "role": "",
            "is_staff": false,
            "is_superuser": false,
            ...
        }
    """
    return {
        "Full Name": f"{er_user_details.get('first_name', '')} {er_user_details.get('last_name', '')}".strip(),
        "Username": er_user_details.get("username", ""),
        "Is Superuser": er_user_details.get("is_superuser", False),
    }


def _extract_global_permissions(er_user_permissions):
    """
    ER API sample permissions:
        {
            "eventcategory": [
                "view",
                "add",
                "change"
            ],
            "eventtype": [
                "change",
                "view",
                "add"
            ],
            "event": [
                "change",
                "add",
                "view"
            ],
            "message": [
                "add",
                "view",
                "change"
            ],
            "observation": [
                "delete",
                "view",
                "change",
                "add"
            ],
            ...
        }
    """
    permissions = {}
    if event_category_perm := er_user_permissions.get("eventcategory", []):
        permissions["Event Category"] = sorted(event_category_perm)
    if event_type_perm := er_user_permissions.get("eventtype", []):
        permissions["Event Type"] = sorted(event_type_perm)
    if event_perm := er_user_permissions.get("event", []):
        permissions["Event"] = sorted(event_perm)
    if message_perm := er_user_permissions.get("message", []):
        permissions["Message"] = sorted(message_perm)
    if observation_perm := er_user_permissions.get("observation", []):
        permissions["Observation"] = sorted(observation_perm)
    return permissions


def _extract_category_permissions(er_user_permissions):
    categories = {}
    not_categories = ["eventcategory", "eventtype", "event", "message", "observation", 'patrol', 'patrolconfiguration', 'tsvectormodel', 'community', 'patroltype', 'team', 'eventsource', 'person', 'notificationmethod', 'teammembership', 'eventphoto', 'eventclassfactor', 'eventdetails', 'patrolconfigurationsubjectgroup', 'eventclass', 'eventrelationshiptype', 'eventnotification', 'eventprovider', 'alertrulenotificationmethod', 'eventnote', 'eventrelatedsubject', 'alertruleeventtype', 'membershiptype', 'refreshrecreateeventdetailview', 'dummy_events', 'eventfactor', 'eventrelatedsegments', 'eventsourceevent', 'formbuilderproxy', 'eventgeometry', 'eventfilter', 'smart', 'eventattachment', 'alertrule', 'eventfile', 'eventrelationship', 'event_for_eventsource']
    for key, perms in er_user_permissions.items():
        if key not in not_categories:
            # Assume other keys are event categories
            categories[key] = sorted(perms)
    return categories


def _merge_event_categories_and_type_perms(global_category_permissions, er_event_types):
    """
        ER API sample event types:
            [
                {
                    "id": "c119f06d-a0e4-485a-af1c-af165c62317c",
                    "has_events_assigned": true,
                    "icon": "",
                    "value": "accident_rep",
                    "display": "Accident Report",
                    "ordernum": 0.0001220703125,
                    "is_collection": false,
                    "category": {
                        "id": "6b359461-aa53-4116-bf2c-04cc580de4ef",
                        "value": "monitoring",
                        "display": "Monitoring",
                        "is_active": true,
                        "ordernum": 6.0,
                        "flag": "user",
                        "permissions": [
                            "delete",
                            "read",
                            "update",
                            "create"
                        ]
                    },
                    "icon_id": "accident_rep",
                    "is_active": true,
                    "default_priority": 0,
                    "default_state": "new",
                    "geometry_type": "Point",
                    "resolve_time": null,
                    "auto_resolve": false,
                    "url": "https://gundi-dev.staging.pamdas.org/api/v1.0/activity/events/eventtypes/c119f06d-a0e4-485a-af1c-af165c62317c"
                },
                ...
            ]
        """
    categories = defaultdict(lambda: {
        "Event Types": set(),
        "Permissions": set()
    })
    for event_type in er_event_types:
        category_info = event_type.get("category", {})
        if not category_info or not category_info.get("is_active", False):
            continue  # Skip event types without a valid category
        category_name = category_info.get("display")
        category_value = category_info.get("value")
        event_type_name = event_type.get("display")
        if not category_name or not category_value or not event_type_name:
            continue  # Skip incomplete entries
        if category_value in global_category_permissions:
            if not categories[category_name]["Permissions"]:
                # Copy category permissions from global permissions
                categories[category_name]["Permissions"].update(global_category_permissions[category_value])
        else:  # If we got the event type then it has view permission at least
            categories[category_name]["Permissions"].update(["view"])
        # Add event type name to the set
        categories[category_name]["Event Types"].add(event_type_name)

    # Convert sets to lists for final output
    return {
        key: {
            "Event Types": sorted(list(value["Event Types"])),
            "Permissions": sorted(list(value["Permissions"]))
        }
        for key, value in categories.items()
    }


def _extract_subject_groups(er_subject_groups):
    result = {}

    def process_group(group, parent_groups=None):
        if parent_groups is None:
            parent_groups = []

        group_name = group['name']
        subject_names = [subject['name'] for subject in group['subjects']]

        # Add subjects to current group
        if group_name not in result:
            result[group_name] = set(subject_names)
        else:
            result[group_name].update(subject_names)

        # Add subjects to all parent groups
        for parent_group in parent_groups:
            if parent_group not in result:
                result[parent_group] = set(subject_names)
            else:
                result[parent_group].update(subject_names)

        # Process subgroups recursively
        for subgroup in group['subgroups']:
            process_group(subgroup, parent_groups + [group_name])

    # Process each top-level group
    for group in er_subject_groups:
        process_group(group)

    # Convert sets to sorted lists for final output
    return {key: sorted(list(value)) for key, value in result.items()}


def _collect_group_uuids(er_subject_groups):
    """Recursively collect every subject-group UUID from a (possibly nested) tree.

    When `get_subjectgroups(flat=False)` returns a tree, subgroup UUIDs are
    only reachable by walking. Operators may legitimately want to filter by a
    sub-group UUID, so we expose all of them.
    """
    uuids = set()

    def walk(group):
        if group.get("id"):
            uuids.add(group["id"])
        for sub in group.get("subgroups", []):
            walk(sub)

    for group in er_subject_groups:
        walk(group)
    return uuids


async def action_show_permissions(integration: Integration, action_config: ShowPermissionsConfig):
    response = {
      "ui": {
        "widget": "DynamicJSONCard"
      },
      "data": {
          "User Details": {},
          "Global Permissions": {},
          "Event Categories": {},
          "Subject Groups": {}
      }
    }
    auth_config = get_authentication_config(integration=integration)
    url_parse = urlparse(integration.base_url)
    if not url_parse.hostname:
        response["data"]["User Details"]["error"] = f"Site URL is empty or invalid: '{integration.base_url}'"
        return response

    async with AsyncERClient(
            service_root=f"{url_parse.scheme}://{url_parse.hostname}/api/v1.0",
            username=auth_config.username,
            password=auth_config.password.get_secret_value() if auth_config.password else None,
            token=auth_config.token.get_secret_value() if auth_config.token else None,
            token_url=f"{url_parse.scheme}://{url_parse.hostname}/oauth2/token",
            client_id="das_web_client",
            connect_timeout=DEFAULT_CONNECT_TIMEOUT_SECONDS,
    ) as er_client:
        try:  # Get user details and global permissions from the users/me endpoint
            if auth_config.authentication_type == ERAuthenticationType.TOKEN:
                er_user_details = await er_client.get_me()
            elif auth_config.authentication_type == ERAuthenticationType.USERNAME_PASSWORD:
                token_retrieved = await er_client.login()
                if not token_retrieved:
                    response["data"]["User Details"]["error"] = "Invalid credentials. Please provide a valid username and password in the authentication config."
                    return response
                er_user_details = await er_client.get_me()
            else:
                response["data"]["User Details"]["error"] = "Please select an valid authentication method."
                return response
        except ERClientBadCredentials:
            response["data"]["User Details"]["error"] = "Invalid credentials. Please provide a valid credentials in the authentication config."
            return response
        except httpx.HTTPStatusError as e:
            try:  # ToDo: Handle this inside the er-client and raise ERClientBadCredentials
                json_response = e.response.json()
            except (ValueError, AttributeError):
                json_response = {}
            error_details = json_response.get("error_description") or response.text
            response["data"]["User Details"]["error"] = f"ER status {e.response.status_code}: {error_details}"
            return response
        except Exception as e:
            response["data"]["User Details"]["error"] = f"Error retrieving user details: {type(e).__name__}:{e}"
            return response  # Cannot continue without a valid user/token
        response["data"]["User Details"] = _extract_user_details(er_user_details=er_user_details)
        user_global_permissions = er_user_details.get("permissions", {})
        response["data"]["Global Permissions"] = _extract_global_permissions(er_user_permissions=user_global_permissions)
        global_category_permissions = _extract_category_permissions(er_user_permissions=user_global_permissions)
        try:  # Get event categories and types from the activity/events/eventtypes endpoint
            event_types_response = await er_client.get_event_types()
        except Exception as e:
            response["data"]["Event Categories"]["error"] = f"Error retrieving event categories: {type(e).__name__}:{e}"
        else:
            response["data"]["Event Categories"] = _merge_event_categories_and_type_perms(
                global_category_permissions=global_category_permissions,
                er_event_types=event_types_response
            )
            response["data"]["Event Type Slugs"] = sorted({
                et["value"] for et in event_types_response if et.get("value")
            })
            response["data"]["Event Category Slugs"] = sorted({
                et["category"]["value"]
                for et in event_types_response
                if (et.get("category") or {}).get("value")
            })
        try:  # Get Subject Groups from the subjectgroups/ endpoint
            include_subjects_from_subgroups_in_parent = action_config.include_subjects_from_subgroups_in_parent
            subject_groups_response = await er_client.get_subjectgroups(
                flat=not include_subjects_from_subgroups_in_parent
            )
        except Exception as e:
            response["data"]["Subject Groups"]["error"] = f"Error retrieving subject groups: {type(e).__name__}:{e}"
        else:
            response["data"]["Subject Groups"] = _extract_subject_groups(er_subject_groups=subject_groups_response)
            response["data"]["Subject Group UUIDs"] = sorted(
                _collect_group_uuids(subject_groups_response)
            )
    return response


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
        token=auth_config.token.get_secret_value() if auth_config.token else None,
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
    # Process events in batches. Per-event state in Redis (keyed by ER event UUID)
    # distinguishes never-seen events (post as new) from previously-forwarded events
    # whose updated_at has advanced (emit one update_event per detected change).
    events_new = 0
    events_updated = 0  # distinct events that had at least one change emitted
    updates_emitted = 0  # individual update_event calls (notes + field changes)
    events_skipped_unchanged = 0
    async with er_client as earth_ranger:
        # One get_event_types() call powers two things:
        #   1) Operator-configured event_type / event_category slugs must be
        #      resolved to UUIDs before being sent in the ER filter blob —
        #      ER filters by event_type__id__in, not by slug.
        #   2) Titleless events fall back to the EventType display name.
        # Best-effort: if the lookup fails we degrade gracefully.
        event_type_maps = await _fetch_event_type_maps(earth_ranger)
        event_type_display_by_slug = event_type_maps.display_by_slug

        # Resolve configured slugs → ER UUIDs for the filter.
        resolved_type_ids, unresolved_types = _resolve_slugs(
            pull_config.event_types, event_type_maps.id_by_slug
        )
        resolved_category_ids, unresolved_categories = _resolve_slugs(
            pull_config.event_categories, event_type_maps.category_id_by_slug
        )
        if unresolved_types or unresolved_categories:
            await log_action_activity(
                integration_id=integration_id,
                action_id="pull_events",
                title="Some configured event-type/category slugs do not exist on this ER instance",
                level=LogLevel.WARNING,
                data={
                    "unresolved_event_types": unresolved_types,
                    "unresolved_event_categories": unresolved_categories,
                },
            )
        # If the operator configured filters and NONE resolve, the safer action
        # is to skip the pull rather than silently fetch everything (which is
        # what ER does when the filter blob is dropped). Watermark is NOT
        # advanced so a fix to the typo / permission can re-pull the window.
        if pull_config.event_types and not resolved_type_ids:
            await log_action_activity(
                integration_id=integration_id,
                action_id="pull_events",
                title="No configured event_types resolved to ER UUIDs; skipping pull",
                level=LogLevel.ERROR,
                data={"configured_event_types": list(pull_config.event_types)},
            )
            return {
                "events_extracted": 0,
                "events_updated": 0,
                "updates_emitted": 0,
                "events_skipped_unchanged": 0,
                "skipped_reason": "no_resolvable_event_types",
            }
        if pull_config.event_categories and not resolved_category_ids:
            await log_action_activity(
                integration_id=integration_id,
                action_id="pull_events",
                title="No configured event_categories resolved to ER UUIDs; skipping pull",
                level=LogLevel.ERROR,
                data={"configured_event_categories": list(pull_config.event_categories)},
            )
            return {
                "events_extracted": 0,
                "events_updated": 0,
                "updates_emitted": 0,
                "events_skipped_unchanged": 0,
                "skipped_reason": "no_resolvable_event_categories",
            }

        date_filter_key = ER_EVENT_FILTER_KEY_BY_DATE_FIELD[pull_config.filter_date_field]
        event_filter = {
            date_filter_key: {"lower": start_datetime}
        }
        if pull_config.end_datetime:
            event_filter[date_filter_key]["upper"] = pull_config.end_datetime
        if resolved_type_ids:
            event_filter["event_type"] = resolved_type_ids
        if resolved_category_ids:
            event_filter["event_category"] = resolved_category_ids
        json_filter = json.dumps(event_filter)
        logger.info(f"Extracting events with filter '{event_filter}'...")

        async for event_batch in earth_ranger.get_events(filter=json_filter, batch_size=BATCH_SIZE):
            for er_event in event_batch:
                er_event_uuid = er_event.get("id")
                if not er_event_uuid:
                    logger.warning("ER event payload missing 'id'; skipping.", extra={"event": er_event})
                    continue
                state_record = await state_manager.get_state(
                    integration_id=integration_id,
                    action_id="pull_events",
                    source_id=er_event_uuid,
                )
                if not state_record.get("gundi_object_id"):
                    # Never seen this ER event before → post it to Gundi as new.
                    transformed = transform_events_to_gundi_schema(
                        events=[er_event],
                        event_type_display_by_slug=event_type_display_by_slug,
                    )
                    if not transformed:
                        continue
                    response = await send_events_to_gundi(
                        events=transformed, integration_id=integration_id
                    )
                    gundi_object_id = _extract_object_id_from_post_events_response(response)
                    if not gundi_object_id:
                        logger.error(
                            "Could not extract object_id from post_events response; "
                            "skipping state persistence for this event.",
                            extra={"er_event_id": er_event_uuid, "response": response},
                        )
                        continue
                    # Mark all existing notes as already-seen (no bulk-forward on first sight).
                    note_ids = [n["id"] for n in er_event.get("notes") or [] if n.get("id")]
                    await _save_event_state(
                        integration_id=integration_id,
                        er_event_uuid=er_event_uuid,
                        gundi_object_id=gundi_object_id,
                        er_event=er_event,
                        seen_note_ids=note_ids,
                    )
                    events_new += 1
                    continue

                # Seen before. Defensive freshness check: same updated_at → no work to do.
                if state_record.get("updated_at") == er_event.get("updated_at"):
                    events_skipped_unchanged += 1
                    continue

                # Updated event → emit one update_event per detected change.
                emitted, new_seen_note_ids = await _emit_event_updates(
                    er_event=er_event,
                    state_record=state_record,
                    integration_id=integration_id,
                )
                updates_emitted += emitted
                if emitted > 0:
                    events_updated += 1
                # Refresh state to reflect what we forwarded this run.
                await _save_event_state(
                    integration_id=integration_id,
                    er_event_uuid=er_event_uuid,
                    gundi_object_id=state_record["gundi_object_id"],
                    er_event=er_event,
                    seen_note_ids=new_seen_note_ids,
                )
    # Save watermark.
    state = {"last_execution": execution_timestamp}
    logger.debug(f"Saving watermark for integration {integration}, action pull_events:\n{state}")
    await state_manager.set_state(
        integration_id=integration_id,
        action_id="pull_events",
        state=state
    )
    logger.info(
        f"pull_events done. new={events_new} updated={events_updated} "
        f"updates_emitted={updates_emitted} skipped_unchanged={events_skipped_unchanged}"
    )
    return {
        "events_extracted": events_new,
        "events_updated": events_updated,
        "updates_emitted": updates_emitted,
        "events_skipped_unchanged": events_skipped_unchanged,
    }


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
        token=auth_config.token.get_secret_value() if auth_config.token else None,
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
        source_id_set = await _resolve_source_ids(
            earth_ranger, group_ids=pull_config.subject_group_ids
        )
        if pull_config.subject_group_ids and not source_id_set:
            await log_action_activity(
                integration_id=integration_id,
                action_id="pull_observations",
                title="Configured subject groups resolved to zero active sources",
                level=LogLevel.ERROR,
                data={"subject_group_ids": pull_config.subject_group_ids},
            )
            # State is intentionally NOT updated — preserves the previous watermark
            # so a fix on the operator side can re-pull the window.
            return {
                "observations_extracted": 0,
                "filter_active": True,
                "sources_resolved": 0,
            }

        filter_active = bool(source_id_set)
        async for observation_batch in earth_ranger.get_observations(**params):
            if filter_active:
                observation_batch = [
                    o for o in observation_batch
                    if str(o.get("source", "")) in source_id_set
                ]
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
    return {
        "observations_extracted": total_observations,
        "filter_active": filter_active,
        "sources_resolved": len(source_id_set) if filter_active else None,
    }


async def _resolve_source_ids(er_client, group_ids):
    """Resolve subject-group UUIDs to a set of source UUIDs.

    Walks ER's subjectgroup tree recursively (flat=False). When a matched UUID
    is found, every descendant subject is included — matching the operator's
    intuition from `action_show_permissions`, which rolls children up into
    every ancestor. Then queries `subjectsources` in a single batched call
    to find the sources currently assigned to those subjects.

    `er_client.get_subjectgroups(flat=False)` returns a list (not an async
    iterator) — the full tree fits in one response.
    """
    if not group_ids:
        return set()

    wanted = set(group_ids)
    groups = await er_client.get_subjectgroups(flat=False)
    subject_ids = set()

    def walk(group, inherited=False):
        matched = inherited or group.get("id") in wanted
        if matched:
            for subject in group.get("subjects", []):
                subject_ids.add(subject["id"])
        for sub in group.get("subgroups", []):
            walk(sub, inherited=matched)

    for group in groups:
        walk(group)

    if not subject_ids:
        return set()

    assignments = await er_client.get_source_assignments(subject_ids=sorted(subject_ids))
    return {str(a["source"]) for a in assignments if a.get("source")}


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


@dataclass
class EventTypeMaps:
    """Three lookup tables derived from a single ER get_event_types() call.

    - ``display_by_slug``: slug → human-readable display name. Used as the
      title fallback when an ER event has no title of its own.
    - ``id_by_slug``: slug → ER EventType UUID. ER's events endpoint filters
      by ``event_type__id__in`` (UUIDs), not by slug, so operator-supplied
      slugs must be resolved before being sent in the filter blob.
    - ``category_id_by_slug``: category slug → ER EventCategory UUID. Same
      story for ``event_type__category__id__in``.
    """
    display_by_slug: Dict[str, str] = field(default_factory=dict)
    id_by_slug: Dict[str, str] = field(default_factory=dict)
    category_id_by_slug: Dict[str, str] = field(default_factory=dict)


async def _fetch_event_type_maps(er_client) -> EventTypeMaps:
    """Build slug→display, slug→type_id, and category_slug→category_id maps.

    ER's EventType has a ``version`` field — each type is exclusively v1 OR
    v2 — and the two list endpoints filter to their own version only:

    - ``GET /api/v1.0/activity/events/eventtypes``  (filter: version=v1)
    - ``GET /api/v2.0/activity/eventtypes``         (filter: version=v2)

    Neither endpoint returns the other version's types, so we query both and
    merge. v1 responses carry a nested category dict (`id`, `value`, ...);
    v2 responses carry only the category slug. As a fallback for category
    UUIDs we hit ``GET /activity/events/categories`` if the merged maps
    haven't already populated ``category_id_by_slug`` (e.g. an ER instance
    with only v2 event types).

    All three fetches are best-effort and logged independently — a 403 on
    one doesn't break the others. If everything fails, all maps are empty;
    downstream callers treat that as the "skip the pull" signal.
    """
    maps = EventTypeMaps()

    try:
        v1_types = await er_client.get_event_types(version=VERSION_1_0)
    except Exception as e:
        logger.warning(
            "Could not fetch ER v1 event types: %s: %s. "
            "Slug→UUID resolution will fall back to v2-only results.",
            type(e).__name__, e,
        )
    else:
        for et in v1_types:
            _absorb_event_type(maps, et, with_category=True)

    try:
        v2_types = await er_client.get_event_types(version=VERSION_2_0)
    except Exception as e:
        logger.warning(
            "Could not fetch ER v2 event types: %s: %s. "
            "Slug→UUID resolution will fall back to v1-only results.",
            type(e).__name__, e,
        )
    else:
        for et in v2_types:
            # v2 carries category as a bare slug, not as a nested object —
            # we resolve category UUIDs separately below.
            _absorb_event_type(maps, et, with_category=False)

    # If we got no category UUIDs from the v1 event types (e.g. ER has only
    # v2 types defined), the canonical categories endpoint fills the gap.
    if not maps.category_id_by_slug:
        try:
            categories = await er_client.get_event_categories()
        except Exception as e:
            logger.warning(
                "Could not fetch ER event categories: %s: %s. "
                "event_category filter slugs will not resolve.",
                type(e).__name__, e,
            )
        else:
            for cat in categories:
                cat_slug = cat.get("value")
                cat_id = cat.get("id")
                if cat_slug and cat_id:
                    maps.category_id_by_slug.setdefault(cat_slug, cat_id)

    return maps


def _absorb_event_type(maps: EventTypeMaps, et: dict, *, with_category: bool) -> None:
    """Merge one ER event-type record into the shared maps.

    Uses ``setdefault`` so v1 and v2 entries for the same slug (shouldn't
    happen — version is exclusive — but be defensive) don't clobber each
    other; first-seen wins.
    """
    slug = et.get("value")
    if not slug:
        return
    if et.get("display"):
        maps.display_by_slug.setdefault(slug, et["display"])
    if et.get("id"):
        maps.id_by_slug.setdefault(slug, et["id"])
    if with_category:
        category = et.get("category") or {}
        if isinstance(category, dict):
            cat_slug = category.get("value")
            cat_id = category.get("id")
            if cat_slug and cat_id:
                maps.category_id_by_slug.setdefault(cat_slug, cat_id)


def _resolve_slugs(configured_slugs, slug_to_id):
    """Split ``configured_slugs`` into (resolved_ids, unresolved_slugs).

    Used for both event_types and event_categories — same shape on both sides.
    """
    resolved = []
    unresolved = []
    for slug in configured_slugs:
        if slug in slug_to_id:
            resolved.append(slug_to_id[slug])
        else:
            unresolved.append(slug)
    return resolved, unresolved


def transform_events_to_gundi_schema(events, event_type_display_by_slug=None):
    """Map an ER event payload list to the Gundi sensors-API event schema.

    ``event_type_display_by_slug`` is an optional {slug: display} map used as
    the title fallback when the ER event has no title of its own. If omitted
    or the slug isn't in the map, falls through to the slug itself — matching
    the prior behavior.
    """
    display_map = event_type_display_by_slug or {}
    transformed_data = []
    for event in events:
        try:
            transformed_event = {}
            # Set base fields
            event_type = event.get("event_type")
            if event_type:
                transformed_event["event_type"] = event_type
            # Prefer the ER event's own title; otherwise fall back to the
            # event-type display name; otherwise the slug; otherwise nothing.
            title = event.get("title") or display_map.get(event_type) or event_type
            if title:
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


# Maps an ER event field name to the corresponding key the sensors-API
# EventCreateUpdateSerializer accepts on PATCH (see cdip PR #428). For each
# entry: (er_event_field, serializer_patch_field, state_record_key).
_ER_FIELD_DIFF_MAP = (
    ("state", "status", "state"),      # ER 'state' → cdip 'status'
    ("priority", "priority", "priority"),
    ("title", "title", "title"),
)


def _extract_object_id_from_post_events_response(response):
    """Pull the Gundi-assigned object_id out of a post_events response.

    post_events always wraps in a list on the wire — even for a single event —
    so the response is normally a list of dicts. Be defensive about dict
    responses too in case the API surface ever shifts.
    """
    if isinstance(response, list):
        if not response:
            return None
        first = response[0]
        if isinstance(first, dict):
            return first.get("object_id")
        return None
    if isinstance(response, dict):
        return response.get("object_id")
    return None


async def _save_event_state(integration_id, er_event_uuid, gundi_object_id, er_event, seen_note_ids):
    """Persist per-event state under (pull_events, er_event_uuid)."""
    await state_manager.set_state(
        integration_id=integration_id,
        action_id="pull_events",
        source_id=er_event_uuid,
        state={
            "gundi_object_id": gundi_object_id,
            "updated_at": er_event.get("updated_at"),
            "state": er_event.get("state"),
            "priority": er_event.get("priority"),
            "title": er_event.get("title"),
            "seen_note_ids": list(seen_note_ids),
        },
    )


async def _emit_event_updates(er_event, state_record, integration_id):
    """Emit one Gundi update_event per detected change on a previously-seen event.

    Returns a tuple (emitted_count, new_seen_note_ids). The caller persists
    the updated state after we return so a transient failure mid-loop leaves
    the watermark untouched and the next run can re-detect.
    """
    gundi_object_id = state_record["gundi_object_id"]
    seen_note_ids = list(state_record.get("seen_note_ids", []))
    seen_set = set(seen_note_ids)
    emitted = 0

    # New notes — one update_event each, preserves author + timestamp per comment.
    for note in er_event.get("notes") or []:
        note_id = note.get("id")
        if not note_id or note_id in seen_set:
            continue
        await update_event_in_gundi(
            gundi_object_id=gundi_object_id,
            changes={"notes": [note]},
            integration_id=integration_id,
        )
        seen_note_ids.append(note_id)
        seen_set.add(note_id)
        emitted += 1

    # Field changes — one update_event per changed field.
    for er_field, patch_field, state_key in _ER_FIELD_DIFF_MAP:
        new_value = er_event.get(er_field)
        if new_value == state_record.get(state_key):
            continue
        await update_event_in_gundi(
            gundi_object_id=gundi_object_id,
            changes={patch_field: new_value},
            integration_id=integration_id,
        )
        emitted += 1

    return emitted, seen_note_ids

