from datetime import datetime
from enum import Enum
from typing import List, Optional, Sequence

from pydantic import BaseModel, Field, SecretStr

from app.services.utils import FieldWithUIOptions, GlobalUISchemaOptions, UIOptions
from .core import AuthActionConfiguration, GenericActionConfiguration, PullActionConfiguration, ExecutableActionMixin, ReferenceActionConfiguration


class ERAuthenticationType(str, Enum):
    TOKEN = "token"
    USERNAME_PASSWORD = "username_password"


# Display labels for the portal's Authentication Type dropdown. A bare JSON
# Schema enum carries no labels, so rjsf would render the raw values.
AUTH_TYPE_DISPLAY_NAMES = {
    ERAuthenticationType.TOKEN: "Token",
    ERAuthenticationType.USERNAME_PASSWORD: "Username & Password",
}


class EventFilterDateField(str, Enum):
    """Which ER timestamp on an Event the start/end window applies to.

    Maps to ER's three event filter keys:
      EVENT_TIME → date_range (Event.event_time)
      CREATED_AT → create_date (Event.created_at)
      UPDATED_AT → update_date (Event.updated_at)
    """
    EVENT_TIME = "event_time"
    CREATED_AT = "created_at"
    UPDATED_AT = "updated_at"


class AuthenticateConfig(AuthActionConfiguration, ExecutableActionMixin):
    authentication_type: ERAuthenticationType = Field(
        ERAuthenticationType.TOKEN,
        description="Type of authentication to use."
    )
    username: Optional[str] = Field(
        "",
        example="myuser",
        description="Username used to authenticate against Earth Ranger API",
    )
    password: Optional[SecretStr] = Field(
        "",
        example="mypasswd1234abc",
        description="Password used to authenticate against Earth Ranger API",
        format="password"
    )
    token: Optional[SecretStr] = Field(
        "",
        example="1b4c1e9c-5ee0-44db-c7f1-177ede2f854a",
        description="Token used to authenticate against Earth Ranger API",
        format="password"
    )

    ui_global_options: GlobalUISchemaOptions = GlobalUISchemaOptions(
        order=["authentication_type", "token", "username", "password"],
    )

    class Config:
        @staticmethod
        def schema_extra(schema: dict):
            # Remove token, username, and password from the root properties
            schema["properties"].pop("token", None)
            schema["properties"].pop("username", None)
            schema["properties"].pop("password", None)

            # Replace the enum $ref with an inline oneOf of const/title pairs
            # so the portal shows display labels instead of raw enum values.
            # Generated metadata (description, default) is kept in place so it
            # can't drift from the Field definition; pydantic v1 emits no
            # property-level title for enum fields, so set it here. Stored
            # values are unchanged, so existing configs and the if/then/else
            # below keep working.
            auth_type = schema["properties"]["authentication_type"]
            auth_type.pop("allOf", None)
            auth_type.pop("$ref", None)
            auth_type.setdefault("title", "Authentication Type")
            auth_type["type"] = "string"
            auth_type["oneOf"] = [
                {
                    "const": member.value,
                    "title": AUTH_TYPE_DISPLAY_NAMES.get(
                        member, member.value.replace("_", " ").title()
                    ),
                }
                for member in ERAuthenticationType
            ]
            # Note: the ERAuthenticationType entry in "definitions" stays —
            # pydantic v1 attaches definitions after schema_extra runs — but
            # it's unreferenced now, so rjsf ignores it.

            # Show token OR username & password based on authentication_type
            schema.update({
                "if": {
                    "properties": {
                        "authentication_type": {"const": "token"}
                    }
                },
                "then": {
                    "required": ["token"],
                    "properties": {
                        "token": {
                            "title": "Token",
                            "description": "Token used to authenticate against Earth Ranger API",
                            "default": "",
                            "example": "1b4c1e9c-5ee0-44db-c7f1-177ede2f854a",
                            "format": "password",
                            "type": "string",
                            "writeOnly": True
                        }
                    }
                },
                "else": {
                    "required": ["username", "password"],
                    "properties": {
                        "username": {
                            "title": "Username",
                            "description": "Username used to authenticate against Earth Ranger API",
                            "default": "",
                            "example": "myuser",
                            "type": "string"
                        },
                        "password": {
                            "title": "Password",
                            "description": "Password used to authenticate against Earth Ranger API",
                            "default": "",
                            "example": "mypasswd1234abc",
                            "format": "password",
                            "type": "string",
                            "writeOnly": True
                        }
                    }
                }
            })


class ShowPermissionsConfig(GenericActionConfiguration, ExecutableActionMixin):
    include_subjects_from_subgroups_in_parent: bool = Field(
        True,
        title="Include Subjects from Subgroups in Parent Group",
        description="When showing subjects from a group, include subjects from its subgroups as well."
    )


class PullObservationsConfig(PullActionConfiguration):
    start_datetime: str = FieldWithUIOptions(
        ...,
        title="Start Datetime",
        description=(
            "ISO-8601 timestamp filtering observations by their recorded_at. "
            "Used only on the FIRST run, or whenever 'force_run_since_start' is true. "
            "After a successful run the action tracks its own watermark and ignores this value "
            "until the next manual reset."
        ),
        format="date-time",
        ui_options=UIOptions(widget="date-time"),
    )
    end_datetime: Optional[str] = FieldWithUIOptions(
        None,
        title="End Datetime",
        description=(
            "Optional ISO-8601 ceiling on recorded_at. Sent to ER on every run, even after the "
            "internal watermark has advanced — leave empty for ongoing pulls and only set it for "
            "bounded historical backfills. After a bounded backfill finishes, clear this field "
            "(otherwise every later run recomputes an empty window and logs an empty completion)."
        ),
        format="date-time",
        ui_options=UIOptions(widget="date-time"),
    )
    force_run_since_start: bool = FieldWithUIOptions(
        False,
        title="Force Run From Start Datetime",
        description=(
            "Resets the internal watermark for one run, so the next pull starts at "
            "'start_datetime' instead. Toggle off again after the catch-up run completes — "
            "otherwise every subsequent run will re-pull from start_datetime."
        ),
    )
    subject_group_ids: List[str] = Field(
        default_factory=list,
        title="Subject Group UUIDs",
        description=(
            "List of ER subject-group UUIDs whose members' observations should be included. "
            "Picking a parent group includes its sub-groups' subjects (resolved recursively). "
            "Run the 'show_permissions' action to find UUIDs available to this account. "
            "An empty list applies no group constraint."
        ),
    )
    subwindow_days: int = FieldWithUIOptions(
        1,
        title="Sub-window Size (days)",
        description=(
            "Backfill granularity: the pull processes the [start, end] window in "
            "slices this many days wide, per source, committing progress after each "
            "slice. Smaller values bound memory and re-work on resume; larger values "
            "reduce per-slice overhead. Default 1."
        ),
        ge=1,
        ui_options=UIOptions(widget="updown"),
    )
    continue_immediately: bool = FieldWithUIOptions(
        False,
        title="Continue Immediately (self-re-trigger)",
        description=(
            "When a run hits its time budget with work remaining, immediately "
            "re-trigger the next chunk via PubSub instead of waiting for the next "
            "scheduled tick. Faster backfills, but requires INTEGRATION_COMMANDS_TOPIC "
            "to be configured. Off by default (scheduler-driven catch-up)."
        ),
    )
    # This integration is most often used only as a destination, so scheduled
    # pulling is OFF by default — the scheduler skips this action quietly until
    # an operator opts in. Enable it on integrations that should pull
    # observations from EarthRanger.
    run_on_schedule: bool = Field(
        False,
        title="Run On Schedule",
        description=(
            "When enabled, this action runs automatically on its configured schedule. "
            "Off by default — enable it for integrations that should pull observations from "
            "EarthRanger; leave it off when this integration is used only as a destination."
        ),
    )

    ui_global_options: GlobalUISchemaOptions = GlobalUISchemaOptions(
        order=["start_datetime", "end_datetime", "subject_group_ids", "subwindow_days", "force_run_since_start", "continue_immediately", "run_on_schedule"],
    )


class PullEventsConfig(PullActionConfiguration):
    start_datetime: str = FieldWithUIOptions(
        ...,
        title="Start Datetime",
        description=(
            "ISO-8601 timestamp filtering events by the field selected in "
            "'filter_date_field' (default: updated_at). Used only on the FIRST run, or whenever "
            "'force_run_since_start' is true. After a successful run the action tracks its own "
            "watermark and ignores this value until the next manual reset."
        ),
        format="date-time",
        ui_options=UIOptions(widget="date-time"),
    )
    end_datetime: Optional[str] = FieldWithUIOptions(
        None,
        title="End Datetime",
        description=(
            "Optional ISO-8601 ceiling on the field selected in 'filter_date_field'. "
            "Sent to ER on every run, even after the internal watermark has advanced — leave "
            "empty for ongoing pulls and only set it for bounded historical backfills."
        ),
        format="date-time",
        ui_options=UIOptions(widget="date-time"),
    )
    filter_date_field: EventFilterDateField = FieldWithUIOptions(
        EventFilterDateField.UPDATED_AT,
        title="Filter By Date Field",
        description=(
            "Which ER timestamp the start/end window applies to. "
            "'updated_at' (the default) catches backdated events and late edits on subsequent "
            "runs because the watermark compares against when ER last touched the row. "
            "'created_at' is similar but ignores edits after the event first appears. "
            "'event_time' filters by when the event actually occurred and will silently miss "
            "events whose event_time predates the current watermark — use only for bounded "
            "historical backfills."
        ),
    )
    force_run_since_start: bool = FieldWithUIOptions(
        False,
        title="Force Run From Start Datetime",
        description=(
            "Resets the internal watermark for one run, so the next pull starts at "
            "'start_datetime' instead. Toggle off again after the catch-up run completes — "
            "otherwise every subsequent run will re-pull from start_datetime."
        ),
    )
    include_attachments: bool = FieldWithUIOptions(
        False,
        title="Forward Event Attachments",
        description=(
            "When enabled, files attached to ER events (photos, documents) are "
            "forwarded to Gundi as event attachments, so destinations that "
            "support them (e.g. EarthRanger, CMORE) receive the files. Files "
            "added to an event after it was first forwarded are picked up on "
            "subsequent runs. Off by default: enabling changes what existing "
            "downstream destinations receive."
        ),
    )
    event_types: List[str] = Field(
        default_factory=list,
        title="Event Types",
        description=(
            "ER event-type slugs to pull. Add one slug per item, e.g. wildlife_sighting_rep. "
            "Run the 'show_permissions' action to see the slugs available for this account. "
            "Combined with Event Categories using ER's AND semantics. "
            "Leave empty to apply no event-type constraint."
        ),
    )
    event_categories: List[str] = Field(
        default_factory=list,
        title="Event Categories",
        description=(
            "ER event-category slugs. Add one slug per item, e.g. wildlife. "
            "ER applies type and category filters with AND semantics. "
            "Leave empty to apply no category constraint."
        ),
    )
    # This integration is most often used only as a destination, so scheduled
    # pulling is OFF by default — the scheduler skips this action quietly until
    # an operator opts in. Enable it on integrations that should pull events
    # from EarthRanger.
    run_on_schedule: bool = Field(
        False,
        title="Run On Schedule",
        description=(
            "When enabled, this action runs automatically on its configured schedule. "
            "Off by default — enable it for integrations that should pull events from "
            "EarthRanger; leave it off when this integration is used only as a destination."
        ),
    )

    ui_global_options: GlobalUISchemaOptions = GlobalUISchemaOptions(
        order=["start_datetime", "end_datetime", "filter_date_field", "event_types", "event_categories", "force_run_since_start", "include_attachments", "run_on_schedule"],
    )

    @classmethod
    def ui_schema(cls, *args, **kwargs):
        """Annotate the slug-list fields with gundi:reference so the portal
        renders each list item as a live dropdown fed by this integration's
        own reference actions (target "self" — the form being configured IS
        the ER integration that can answer). Deliberately no ui:widget:
        portals without reference support keep plain text inputs. The
        annotation sits on the array's ``items`` node so rjsf applies it to
        every element. Spec:
        https://github.com/PADAS/gundi-integration-cmore/blob/main/docs/superpowers/specs/2026-07-31-reference-data-config-ui-design.md
        """
        base = super().ui_schema(*args, **kwargs)
        base["event_types"] = {"items": {"gundi:reference": _reference("list_event_types")}}
        base["event_categories"] = {"items": {"gundi:reference": _reference("list_event_categories")}}
        return base


def _reference(action: str, params: Optional[dict] = None) -> dict:
    """Build a gundi:reference ui_schema annotation (mirrors the helper in
    gundi-integration-cmore's configurations.py; ER only needs target "self")."""
    return {
        "action": action,
        "target": "self",
        "params": params or {},
        "allow_free_text": True,
    }


class ListEventTypesQuery(ReferenceActionConfiguration):
    """List the ER event types visible to this integration's credentials."""


class ListEventCategoriesQuery(ReferenceActionConfiguration):
    """List the ER event categories visible to this integration's credentials."""


class ListSubjectTypesQuery(ReferenceActionConfiguration):
    """List the subject subtypes observed on this ER site (plus their parent
    subject types as fallback options)."""


class ListEventTypeFieldsQuery(ReferenceActionConfiguration):
    event_type: str


class ListEventFieldValuesQuery(ReferenceActionConfiguration):
    event_type: str
    field_key: str


def share_preview(action: str, kind: str, selection_fields: Sequence[str]) -> dict:
    """gundi:share_preview annotation: before saving this action, the portal shows
    what will be shared, read from the reference action `action` with the
    section's current values. `kind` is "events" or "subjects"; `selection_fields`
    are the fields the user picks in (all empty means share everything)."""
    return {"action": action, "kind": kind, "selection_fields": list(selection_fields)}


RAW_VIEW_TITLE = "Legacy View"

# v1 er2er state key -> form field path ("a.b" is a nested field). The portal's
# raw view translates a pasted v1 state into the form, and the form back, with these.
EVENTS_V1_KEYS = {
    "prepend_system_to_categories": "compatibility.prepend_system_to_categories",
    "prepend_system_to_event_types": "compatibility.prepend_system_to_event_types",
    "prepend_system_to_event_titles": "compatibility.prepend_system_to_event_titles",
    "update_schema": "compatibility.update_schema",
    "delete_unmatched_events": "compatibility.delete_unmatched_events",
    "source_system_value": "compatibility.source_system_value",
    "source_system_name": "compatibility.source_system_name",
    "within_featuregroups": "area_feature_groups",
}
OBSERVATIONS_V1_KEYS = {
    "subject_group": "destination_subject_group",
    "feature_groups": "area_feature_groups",
    "provider_key": "compatibility.provider_key",
    "subject_additional": "compatibility.subject_additional",
    "src_user": "compatibility.src_user",
}
# v1 state keys with no form field are kept in the config as they are, except
# these. Dropped: v1's own run status and sync window, which v2 doesn't read.
# Masked: kept, but saved as "*******" so the secret never reaches the config.
# dest_endpoint is dropped: destinations come from the connection, not from the share's settings.
EVENTS_V1_DROP = ["days_to_sync", "dest_endpoint", "error", "last_run", "last_successful_run", "warning"]
OBSERVATIONS_V1_DROP = ["batch_size", "error", "last_run", "last_successful_run", "look_back_window_hours",
                        "schedule_interval_minutes"]
V1_MASK = ["dest_token"]
# Unwrapped: v1 stores these lists as [{"id": ...}]; the portal reads the named field of each item.
OBSERVATIONS_V1_UNWRAP = {"feature_groups": "id"}


def raw_view(v1_keys: dict, drop: Sequence[str] = (), mask: Sequence[str] = (), unwrap: Optional[dict] = None) -> dict:
    """gundi:raw_view annotation: the portal shows a "Legacy View" of this
    action and translates between it and the form with this key map."""
    annotation = {"title": RAW_VIEW_TITLE, "map": dict(v1_keys), "drop": list(drop), "mask": list(mask)}
    if unwrap:
        annotation["unwrap"] = dict(unwrap)
    return annotation


class EventsCompatibility(BaseModel):
    """v1 settings kept for migrated shares. None means "use the simple-mode default"."""

    prepend_system_to_categories: Optional[bool] = Field(None, title="Prefix categories with the source name")
    prepend_system_to_event_types: Optional[bool] = Field(None, title="Prefix event types with the source name")
    prepend_system_to_event_titles: Optional[bool] = Field(None, title="Prefix event titles with the source name")
    update_schema: Optional[bool] = Field(None, title="Allow updates to destination event types")
    delete_unmatched_events: Optional[bool] = Field(
        None, title="Remove destination copies when the source event is deleted"
    )
    source_system_value: Optional[str] = Field(None, title="Source system code")
    source_system_name: Optional[str] = Field(None, title="Source system display name")


class ObservationsCompatibility(BaseModel):
    provider_key: Optional[str] = Field(None, title="Destination provider key")
    subject_additional: Optional[bool] = Field(None, title="Copy subject and source 'additional' data")
    src_user: Optional[str] = Field(None, title="Sharing user recorded in the lineage")


class SyncEventsConfig(PullActionConfiguration):
    """Shares this site's events with every destination EarthRanger site. Choose which event types and areas to share."""

    run_on_schedule: bool = Field(
        False, title="Share events", description="Copy new and changed events to every destination."
    )
    event_types: List[str] = Field(
        default_factory=list, title="Event types to share", description="Leave empty to share all event types."
    )
    area_feature_groups: List[str] = Field(
        default_factory=list,
        title="Events area filter",
        description="Only share events inside these feature groups. Leave empty to share events anywhere.",
    )
    start_from: Optional[datetime] = Field(
        None, title="Start from", description="Where the first sync starts when no progress is recorded yet."
    )
    compatibility: EventsCompatibility = Field(
        default_factory=EventsCompatibility, title="Advanced (v1 compatibility)"
    )

    ui_global_options: GlobalUISchemaOptions = GlobalUISchemaOptions(
        order=["run_on_schedule", "event_types", "area_feature_groups", "compatibility", "start_from"],
    )

    @classmethod
    def ui_schema(cls, *args, **kwargs):
        base = super().ui_schema(*args, **kwargs)
        base["event_types"] = {"items": {"gundi:reference": _reference("list_share_event_types")}}
        base["area_feature_groups"] = {"items": {"gundi:reference": _reference("list_feature_groups")}}
        base["start_from"] = {"ui:widget": "hidden"}
        base["gundi:raw_view"] = raw_view(EVENTS_V1_KEYS, drop=EVENTS_V1_DROP, mask=V1_MASK)
        base["gundi:share_preview"] = share_preview("preview_events_share", "events", ["event_types"])
        return base


class SyncObservationsConfig(PullActionConfiguration):
    """Shares this site's subjects and their tracks with every destination EarthRanger site. Choose which subject groups and areas to share."""

    run_on_schedule: bool = Field(
        False, title="Share subject tracks", description="Copy new track points to every destination."
    )
    subject_groups: List[str] = Field(
        default_factory=list, title="Subject groups to share", description="Leave empty to share all subject groups."
    )
    destination_subject_group: Optional[str] = Field(
        None,
        title="Destination subject group name",
        description="Shared subjects are added to this group at each destination. "
                    "Leave it empty to use the source site's name.",
    )
    area_feature_groups: List[str] = Field(
        default_factory=list,
        title="Tracks area filter",
        description="Only share points inside these feature groups. Leave empty to share points anywhere.",
    )
    start_from: Optional[datetime] = Field(
        None, title="Start from",
        description="For a subject's first sync only: shares points recorded after this time, "
                    "within the last 12 hours. An earlier time has no effect.",
    )
    compatibility: ObservationsCompatibility = Field(
        default_factory=ObservationsCompatibility, title="Advanced (v1 compatibility)"
    )

    ui_global_options: GlobalUISchemaOptions = GlobalUISchemaOptions(
        order=["run_on_schedule", "subject_groups", "destination_subject_group", "area_feature_groups",
               "compatibility", "start_from"],
    )

    @classmethod
    def ui_schema(cls, *args, **kwargs):
        base = super().ui_schema(*args, **kwargs)
        base["subject_groups"] = {"items": {"gundi:reference": _reference("list_subject_groups")}}
        base["area_feature_groups"] = {"items": {"gundi:reference": _reference("list_feature_groups")}}
        base["start_from"] = {"ui:widget": "hidden"}
        base["gundi:raw_view"] = raw_view(OBSERVATIONS_V1_KEYS, drop=OBSERVATIONS_V1_DROP, mask=V1_MASK,
                                           unwrap=OBSERVATIONS_V1_UNWRAP)
        base["gundi:share_preview"] = share_preview("preview_subjects_share", "subjects", ["subject_groups"])
        return base

class ListShareEventTypesQuery(ReferenceActionConfiguration):
    """The site's active event types, v2 and v1, for Share Events (v1 types are shared too)."""


class ListSubjectGroupsQuery(ReferenceActionConfiguration):
    """The site's subject groups."""


class ListFeatureGroupsQuery(ReferenceActionConfiguration):
    """The site's spatial feature groups."""


class PreviewEventsShareQuery(ReferenceActionConfiguration):
    """What Share Events would share with these values: the portal sends the
    section's whole form; the event type selection and the area filter are read."""
    event_types: List[str] = Field(default_factory=list)
    area_feature_groups: List[str] = Field(default_factory=list)


class PreviewSubjectsShareQuery(ReferenceActionConfiguration):
    """What Share Subjects would share with these values: the portal sends the
    section's whole form; the subject group selection and the area filter are read."""
    subject_groups: List[str] = Field(default_factory=list)
    area_feature_groups: List[str] = Field(default_factory=list)
