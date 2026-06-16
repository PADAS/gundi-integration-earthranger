import json
import datetime
import logging
import time
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Dict
from urllib.parse import urlparse

import httpx
import stamina
from dateutil import parser as dateutil_parser
from erclient import AsyncERClient, ERClientException, VERSION_1_0, VERSION_2_0
from erclient.er_errors import ERClientBadCredentials
from gundi_client_v2.client import GundiClient
from gundi_core.events import LogLevel
from gundi_core.schemas.v2 import Integration
from app import settings
from app.services.utils import find_config_for_action
from app.services.state import IntegrationStateManager
from .configurations import AuthenticateConfig, EventFilterDateField, PullObservationsConfig, PullEventsConfig, \
    ERAuthenticationType, ShowPermissionsConfig
from .source_profiles import SourceProfileResolver
from ..services.activity_logger import activity_logger, log_action_activity
from ..services.gundi import send_events_to_gundi, send_observations_to_gundi, update_event_in_gundi
from ..services.action_scheduler import trigger_action

logger = logging.getLogger(__name__)


DEFAULT_CONNECT_TIMEOUT_SECONDS = 10.0
BATCH_SIZE = 100
SUBJECT_ID_CHUNK_SIZE = 25
BUDGET_FRACTION = 0.8            # fraction of MAX_ACTION_EXECUTION_TIME before yielding
MAX_NO_PROGRESS_RETRIES = 3      # self-re-trigger runaway guard
LOCK_MARGIN_SECONDS = 30         # lease TTL margin above the hard timeout
BACKFILL_LOCK_SOURCE_ID = "backfill-lock"
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
            event_types_response = _as_list(await er_client.get_event_types())
        except Exception as e:
            response["data"]["Event Categories"]["error"] = f"Error retrieving event categories: {type(e).__name__}:{e}"
        else:
            event_types_response = [et for et in event_types_response if isinstance(et, dict)]
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
    # UI root for deep-link URLs stamped onto each event's provider_metadata so
    # downstream destinations (CMORE) can render a click-through back to the
    # source ER event.
    er_ui_root = _er_ui_root(url_parse)
    # Diagnostic: surface the computed UI root so an empty value (e.g. from a
    # misconfigured base_url) explains a missing deep-link in downstream
    # destinations like CMORE.
    logger.info(
        "pull_events: integration.base_url=%r → er_ui_root=%r",
        integration.base_url, er_ui_root,
    )
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

        # include_notes is required: ER's events-list endpoint omits the notes
        # array unless explicitly requested, so without this each er_event comes
        # back without notes and no note update_event is ever emitted (the
        # ER-note → downstream-comment path silently never fires).
        async for event_batch in earth_ranger.get_events(
            filter=json_filter, batch_size=BATCH_SIZE, include_notes=True
        ):
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
                        er_ui_root=er_ui_root,
                    )
                    if not transformed:
                        continue
                    # Diagnostic: log what we're about to POST so a downstream
                    # destination seeing an unexpected payload (e.g. CMORE
                    # rendering provider_metadata=None) can be traced back to
                    # the ER runner's outbound shape.
                    logger.info(
                        "Posting Gundi event: er_event_uuid=%r title=%r "
                        "provider_metadata=%r",
                        er_event_uuid,
                        transformed[0].get("title"),
                        transformed[0].get("provider_metadata"),
                    )
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
    execution_timestamp = datetime.datetime.now(tz=datetime.timezone.utc).isoformat()
    pull_config = action_config
    auth_config = get_authentication_config(integration=integration)
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

    start_monotonic = time.monotonic()
    soft_budget = settings.MAX_ACTION_EXECUTION_TIME * BUDGET_FRACTION

    async with er_client as earth_ranger:
        # Mutual exclusion: a long backfill may still be running when the next
        # scheduled tick fires. Without the lease, both would process the same
        # cursor units concurrently (duplicate sends + cursor races).
        if not await _acquire_backfill_lease(integration_id):
            return _skip_quietly(
                integration_id, "pull_observations",
                reason="backfill_in_progress",
                message="Skipping 'pull_observations': another run holds the backfill lease.",
                log_level=logging.INFO,
            )
        try:
            # One resolver per run: lazily fetches + caches per-source profiles
            # (manufacturer_id, subject assignment history) so observations are
            # labelled with the device's natural id and time-correct subject name.
            resolver = SourceProfileResolver(earth_ranger, integration_id=integration_id)
            state = await state_manager.get_state(
                integration_id=integration_id, action_id="pull_observations"
            )
            cursor = state.get("backfill")
            last_execution = state.get("last_execution")

            if cursor is None:
                # Fresh run: compute the window and resolve the source list once.
                last = state.get("last_execution")
                if not last or pull_config.force_run_since_start:
                    window_start = pull_config.start_datetime
                else:
                    window_start = last
                window_end = pull_config.end_datetime or execution_timestamp

                source_id_set = await _resolve_source_ids(
                    earth_ranger,
                    group_ids=pull_config.subject_group_ids,
                    integration_id=integration_id,
                )
                if pull_config.subject_group_ids and not source_id_set:
                    await log_action_activity(
                        integration_id=integration_id,
                        action_id="pull_observations",
                        title="Configured subject groups resolved to zero active sources",
                        level=LogLevel.ERROR,
                        data={"subject_group_ids": pull_config.subject_group_ids},
                    )
                    # Watermark intentionally NOT advanced; operator fix re-pulls.
                    return {
                        "status": "skipped_no_sources",
                        "observations_extracted": 0,
                        "filter_active": True,
                        "sources_resolved": 0,
                    }
                cursor = _build_backfill_cursor(
                    start=window_start,
                    end=window_end,
                    subwindow_days=pull_config.subwindow_days,
                    source_ids=source_id_set,
                )

            filter_active = cursor["sources"] != [None]
            subwindows = _iter_subwindows(
                cursor["start"], cursor["end"], cursor["subwindow_days"]
            )

            total_observations = 0
            units_completed = 0
            wi = cursor["window_index"]
            si = cursor["source_index"]

            while wi < len(subwindows):
                w_start, w_end = subwindows[wi]
                while si < len(cursor["sources"]):
                    # Yield before starting a unit if the soft budget is spent.
                    if time.monotonic() - start_monotonic >= soft_budget:
                        cursor["window_index"] = wi
                        cursor["source_index"] = si
                        # Tracks consecutive zero-progress yields. Only consulted
                        # by the self-re-trigger guard below (continue_immediately);
                        # in scheduler-driven mode the scheduler cadence is the brake.
                        cursor["no_progress_count"] = (
                            cursor.get("no_progress_count", 0) + 1
                            if units_completed == 0 else 0
                        )
                        await _save_backfill_cursor(
                            integration_id, last_execution=last_execution, cursor=cursor
                        )
                        logger.info(
                            "pull_observations yielding (budget): window %d/%d source %d/%d",
                            wi, len(subwindows), si, len(cursor["sources"]),
                        )
                        # Opt-in: immediately re-trigger the next chunk via PubSub,
                        # unless we're making no progress (runaway guard). Under
                        # TRIGGER_ACTIONS_ALWAYS_SYNC (local/test), the re-triggered
                        # run is a no-op: it runs inline before this run's finally
                        # releases the lease, so it skips on the held lease.
                        if pull_config.continue_immediately:
                            if cursor["no_progress_count"] < MAX_NO_PROGRESS_RETRIES:
                                try:
                                    await trigger_action(integration_id, "pull_observations")
                                except Exception as exc:
                                    # Cursor is already saved; a failed re-trigger is
                                    # non-fatal — the next scheduled tick resumes from it.
                                    logger.warning(
                                        "pull_observations: re-trigger failed (%s); next "
                                        "chunk will resume on the scheduled tick.",
                                        exc,
                                        extra={"attention_needed": True},
                                    )
                            else:
                                logger.warning(
                                    "pull_observations not re-triggering: %d consecutive "
                                    "no-progress runs (runaway guard).",
                                    cursor["no_progress_count"],
                                    extra={"attention_needed": True},
                                )
                        return {
                            "status": "in_progress",
                            "observations_extracted": total_observations,
                            "units_failed": cursor.get("units_failed", 0),
                            "window_index": wi,
                            "source_index": si,
                            "filter_active": filter_active,
                            "sources_resolved": len(cursor["sources"]) if filter_active else None,
                        }
                    source = cursor["sources"][si]
                    try:
                        total_observations += await _pull_source_window(
                            earth_ranger, source, w_start, w_end,
                            integration_id=integration_id, resolver=resolver,
                        )
                    except Exception as e:
                        # Don't wedge the backfill on one bad unit: log loudly and
                        # advance past it (at-least-once; operator can re-pull).
                        cursor["units_failed"] = cursor.get("units_failed", 0) + 1
                        logger.error(
                            "pull_observations unit failed (source=%r window=%s..%s): %s",
                            source, w_start, w_end, e,
                            extra={"attention_needed": True},
                        )
                    si += 1
                    units_completed += 1
                    cursor["window_index"] = wi
                    cursor["source_index"] = si
                    await _save_backfill_cursor(
                        integration_id, last_execution=last_execution, cursor=cursor
                    )
                si = 0
                wi += 1

            # All units done → advance the watermark to the window end and clear
            # the cursor (drops "backfill", sets last_execution).
            units_failed = cursor.get("units_failed", 0)
            # Advance the watermark first (durable record of completion) so a
            # failure publishing the warning below can't force a full re-run.
            await state_manager.set_state(
                integration_id=integration_id,
                action_id="pull_observations",
                state={"last_execution": cursor["end"]},
            )
            if units_failed:
                await log_action_activity(
                    integration_id=integration_id,
                    action_id="pull_observations",
                    title="Observation backfill completed with skipped units",
                    level=LogLevel.WARNING,
                    data={"units_failed": units_failed, "window_end": cursor["end"]},
                )
            logger.info(
                f"Extracted {total_observations} observations for integration {integration}."
            )
            return {
                "status": "complete",
                "observations_extracted": total_observations,
                "units_failed": units_failed,
                "filter_active": filter_active,
                "sources_resolved": len(cursor["sources"]) if filter_active else None,
            }
        finally:
            await _release_backfill_lease(integration_id)


async def _fetch_source_assignments(er_client, subject_ids, *, integration_id=None):
    """Fetch subjectsources for many subjects, chunked to keep URLs short and
    each response within a single page.

    ER's ``subjectsources`` endpoint is a single (non-paginated) request: a huge
    ``subjects=`` query string risks a 414, and a response large enough to
    paginate would be silently truncated. Chunking subjects into small groups
    sidesteps both. As a safety net we WARN if any chunk response still carries a
    ``next`` (the single-page assumption breaking), and we capture diagnostics on
    unexpected/malformed shapes (problem (b)).
    """
    assignments = []
    malformed = 0
    for chunk in _chunked(subject_ids, SUBJECT_ID_CHUNK_SIZE):
        raw = await er_client.get_source_assignments(subject_ids=chunk)
        if isinstance(raw, dict):
            if raw.get("next"):
                logger.warning(
                    "subjectsources chunk returned a paginated 'next' "
                    "(count=%s, chunk_size=%d) — sources may be dropped; "
                    "lower SUBJECT_ID_CHUNK_SIZE.",
                    raw.get("count"), len(chunk),
                    extra={"attention_needed": True},
                )
            records = raw.get("results", [])
        elif isinstance(raw, list):
            records = raw
        else:
            logger.warning(
                "Unexpected subjectsources response type %s: %.200r",
                type(raw).__name__, raw,
                extra={"attention_needed": True},
            )
            records = []
        if records:
            logger.debug(
                "subjectsources chunk: response_type=%s records=%d sample=%r",
                type(raw).__name__, len(records), records[0],
            )
        for rec in records:
            if isinstance(rec, dict) and rec.get("source"):
                assignments.append(rec)
            else:
                malformed += 1
                logger.warning(
                    "Skipping malformed subjectsource record: %r", rec,
                    extra={"attention_needed": True},
                )
    if malformed and integration_id:
        await log_action_activity(
            integration_id=integration_id,
            action_id="pull_observations",
            title="Some source-assignment records were malformed and skipped",
            level=LogLevel.WARNING,
            data={"malformed_count": malformed},
        )
    return assignments


async def _resolve_source_ids(er_client, group_ids, *, integration_id=None):
    """Resolve subject-group UUIDs to a set of source UUIDs.

    Walks ER's subjectgroup tree recursively (flat=False). When a matched UUID
    is found, every descendant subject is included. Then resolves the subjects'
    current source assignments via ``_fetch_source_assignments`` (chunked, so a
    large subject list neither overruns the URL nor drops paginated rows).
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

    assignments = await _fetch_source_assignments(
        er_client, sorted(subject_ids), integration_id=integration_id
    )
    return {str(a["source"]) for a in assignments if a.get("source")}


async def _acquire_backfill_lease(integration_id):
    """Acquire the per-(integration, pull_observations) lease.

    Returns True if this invocation may proceed, False if another invocation
    currently holds it. The TTL is the crash backstop: because the handler is
    hard-killed by asyncio.wait_for at MAX_ACTION_EXECUTION_TIME, a run can never
    outlive its own lease. Fails OPEN on a state-store error (a rare duplicate is
    cheaper than turning a benign no-op into a crash).
    """
    ttl = int(settings.MAX_ACTION_EXECUTION_TIME) + LOCK_MARGIN_SECONDS
    # NOTE: state_manager.set_if_absent retries on redis.RedisError (stamina),
    # so a hard Redis outage delays this fail-open path by the retry budget.
    try:
        return await state_manager.set_if_absent(
            integration_id=integration_id,
            action_id="pull_observations",
            source_id=BACKFILL_LOCK_SOURCE_ID,
            ttl_seconds=ttl,
        )
    except Exception as e:
        logger.warning(
            "Backfill lease acquire failed (%s); proceeding without lease.", e
        )
        return True


async def _release_backfill_lease(integration_id):
    """Release the lease. Best-effort: if this fails, the TTL expires it."""
    try:
        await state_manager.delete_state(
            integration_id=integration_id,
            action_id="pull_observations",
            source_id=BACKFILL_LOCK_SOURCE_ID,
        )
    except Exception as e:
        logger.warning(
            "Backfill lease release failed (%s); TTL will expire it.", e
        )


def _skip_quietly(integration_id, action_id, *, reason, message, log_level=logging.INFO):
    """Record an expected pull-action skip in the local log only.

    Mirrors ``app.services.action_runner._skip_quietly`` (defined locally to
    avoid a circular import: action_runner imports app.actions at module load).
    Used when an overlapping invocation finds the backfill lease already held —
    an expected, steady-state no-op kept out of the portal activity feed.
    """
    logger.log(log_level, f"{message} (integration '{integration_id}')")
    return {"skipped": True, "reason": reason}


def _build_backfill_cursor(*, start, end, subwindow_days, source_ids):
    """Snapshot the work definition + zeroed progress for a new backfill run.

    ``source_ids`` is snapshotted (sorted) so the unit sequence is stable across
    resumes. An empty set means "no group filter" → a single ``None`` source,
    i.e. one whole-instance fetch per sub-window.
    """
    sources = sorted(source_ids) if source_ids else [None]
    return {
        "start": start,
        "end": end,
        "subwindow_days": int(subwindow_days or 1),
        "sources": sources,
        "window_index": 0,
        "source_index": 0,
        "no_progress_count": 0,
    }


async def _save_backfill_cursor(integration_id, *, last_execution, cursor):
    """Persist the cursor alongside the (unchanged) watermark.

    The watermark is only advanced on completion; until then it is preserved so
    a failure never loses the previously-confirmed window.
    """
    state = {"backfill": cursor}
    if last_execution is not None:
        state["last_execution"] = last_execution
    await state_manager.set_state(
        integration_id=integration_id,
        action_id="pull_observations",
        state=state,
    )


async def _pull_source_window(er_client, source, start, end, *, integration_id, resolver=None):
    """Drain one (source × sub-window) unit and forward to Gundi.

    ``source=None`` means no source filter (whole instance for the window).
    Returns the number of observations forwarded. ER filters server-side, so no
    client-side source filtering is needed. ``er_client`` must already be an
    entered/open client session (call within ``async with er_client``).

    When ``resolver`` is given, each batch's source UUIDs are prefetched (so the
    resolver caches per-source profiles) and passed into the transform to enrich
    observations with ``manufacturer_id``/``source_name``/``subject_type``.
    """
    params = {"start": start, "end": end, "batch_size": BATCH_SIZE}
    if source is not None:
        params["source_id"] = source
    sent = 0
    async for observation_batch in er_client.get_observations(**params):
        if resolver is not None:
            await resolver.ensure({o.get("source") for o in observation_batch if o.get("source")})
        transformed = transform_observations_to_gundi_schema(
            observations=observation_batch, resolver=resolver
        )
        if not transformed:
            continue
        logger.info(f"Sending {len(transformed)} observations to Gundi...")
        await send_observations_to_gundi(observations=transformed, integration_id=integration_id)
        sent += len(transformed)
    return sent


# Auxiliary functions

def _as_list(response):
    """Normalize an ER list response to a plain list of records.

    ER list endpoints (via AsyncERClient, which unwraps the ``data`` envelope)
    return EITHER a flat list OR a paginated envelope
    ``{"count", "next", "previous", "results": [...]}``. Iterating the envelope
    directly walks its string keys and raises
    ``AttributeError: 'str' object has no attribute 'get'``, so every
    list-consuming caller must normalize through here first.

    Note: only the first page is returned — callers that need every page must
    paginate explicitly (the non-generator erclient getters do a single
    request).
    """
    if isinstance(response, dict):
        return response.get("results", [])
    return response or []


def _chunked(seq, size):
    """Yield successive ``size``-length chunks (lists) from ``seq``."""
    seq = list(seq)
    for i in range(0, len(seq), size):
        yield seq[i:i + size]


def _parse_iso(value):
    """Parse an ISO-8601 string (tolerant of ER's no-colon offsets and 'Z')."""
    return dateutil_parser.isoparse(value)


def _ensure_utc(dt):
    """Treat a naive datetime as UTC so naive/aware comparisons never raise."""
    if dt.tzinfo is None:
        return dt.replace(tzinfo=datetime.timezone.utc)
    return dt


def _to_iso(dt):
    """Render a datetime back to ISO-8601."""
    return dt.isoformat()


def _iter_subwindows(start_iso, end_iso, subwindow_days):
    """Return ascending half-open ``[start, end)`` sub-windows as ISO pairs.

    The window list is deterministic given (start, end, subwindow_days), so a
    resumed run regenerates the exact same units and continues by index.
    Returns an empty list when ``start`` is not before ``end``.
    """
    days = max(1, int(subwindow_days or 1))
    start = _ensure_utc(_parse_iso(start_iso))
    end = _ensure_utc(_parse_iso(end_iso))
    delta = datetime.timedelta(days=days)
    windows = []
    cur = start
    while cur < end:
        nxt = min(cur + delta, end)
        windows.append((_to_iso(cur), _to_iso(nxt)))
        cur = nxt
    return windows


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
        for et in _as_list(v1_types):
            if isinstance(et, dict):
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
        for et in _as_list(v2_types):
            # v2 carries category as a bare slug, not as a nested object —
            # we resolve category UUIDs separately below.
            if isinstance(et, dict):
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
            for cat in _as_list(categories):
                if not isinstance(cat, dict):
                    continue
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


def _er_ui_root(url_parse) -> str:
    """Strip path/query/fragment from a parsed ER integration base URL,
    leaving just ``scheme://netloc`` (so ports survive). Returns an empty
    string if the URL is unparseable, which downstream code treats as
    "skip the deep-link" rather than emitting a malformed URL.
    """
    if not (url_parse.scheme and url_parse.netloc):
        return ""
    return f"{url_parse.scheme}://{url_parse.netloc}"


def transform_events_to_gundi_schema(events, event_type_display_by_slug=None, er_ui_root=None):
    """Map an ER event payload list to the Gundi sensors-API event schema.

    ``event_type_display_by_slug`` is an optional {slug: display} map used as
    the title fallback when the ER event has no title of its own. If omitted
    or the slug isn't in the map, falls through to the slug itself — matching
    the prior behavior.

    ``er_ui_root`` is an optional ``scheme://netloc`` string for the ER web
    UI. When supplied, each transformed event's ``provider_metadata.source_event_url``
    points at the source ER event so downstream destinations can render a
    click-through cross-reference. Requires gundi-core 1.12.0+ (the
    ``Event.provider_metadata`` field) and a cdip serializer that accepts it.
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
                # ER events have no per-device source; use the event_type as the
                # source so Gundi groups them sensibly (external_source_id)
                # rather than defaulting everything to "default-source".
                transformed_event["source"] = event_type
            # Prefer the ER event's own title; otherwise fall back to the
            # event-type display name; otherwise the slug; otherwise nothing.
            title = event.get("title") or display_map.get(event_type) or event_type
            if title:
                transformed_event["title"] = title
            # Use the event's own time, not created_at (which is the save
            # time) — CMORE shows recorded_at as the event's dateOccurred.
            if recorded_at := event.get("time") or event.get("created_at"):
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
            # Provider-side metadata surfaced by downstream destinations
            # (e.g. CMORE renders the deep-link as a comment and the serial
            # number in the event title). Each piece is only added when
            # available, and Event has no `additional` field, so this is the
            # channel for passing through provider context like serial_number.
            provider_metadata = {}
            er_event_id = event.get("id")
            if er_ui_root and er_event_id:
                provider_metadata["source_event_url"] = f"{er_ui_root}/events/{er_event_id}"
            serial_number = event.get("serial_number")
            if serial_number is not None:
                provider_metadata["serial_number"] = serial_number
            if provider_metadata:
                transformed_event["provider_metadata"] = provider_metadata
            # Save other fields in additional
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


def transform_observations_to_gundi_schema(observations, resolver=None):
    transformed_data = []
    for observation in observations:
        try:
            transformed_observation = {}
            recorded_at = observation.get("recorded_at")
            if recorded_at:
                transformed_observation["recorded_at"] = recorded_at
            source_uuid = observation.get("source")
            if source_uuid:
                if resolver is not None:
                    recorded_at_norm = recorded_at.replace("Z", "+00:00") if recorded_at else None
                    when = datetime.datetime.fromisoformat(recorded_at_norm) if recorded_at_norm else None
                    resolved = resolver.resolve(source_uuid, when)
                    transformed_observation["source"] = resolved.external_source_id
                    if resolved.source_name:
                        transformed_observation["source_name"] = resolved.source_name
                    if resolved.subject_type:
                        transformed_observation["subject_type"] = resolved.subject_type
                else:
                    transformed_observation["source"] = f"er-src-{source_uuid}"
            if location := observation.get("location"):
                transformed_observation["location"] = {
                    "lon": location.get("longitude"),
                    "lat": location.get("latitude"),
                }
            # Everything not already mapped goes to additional; preserve the raw
            # ER source UUID for traceability/reconciliation after the identity change.
            additional = {
                key: value for key, value in observation.items()
                if key not in transformed_observation.keys() and key not in ("source", "er_source_id")
            }
            if source_uuid:
                additional["er_source_id"] = source_uuid
            transformed_observation["additional"] = additional
        except Exception as e:
            logger.error(
                f"Error transforming observation {observation}: {e}",
                extra={"attention_needed": True},
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

