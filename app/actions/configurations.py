from datetime import datetime
from enum import Enum
from typing import List, Optional

from pydantic import Field, SecretStr

from app.services.utils import GlobalUISchemaOptions
from .core import AuthActionConfiguration, GenericActionConfiguration, PullActionConfiguration, ExecutableActionMixin


class ERAuthenticationType(str, Enum):
    TOKEN = "token"
    USERNAME_PASSWORD = "username_password"


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
    start_datetime: str
    end_datetime: Optional[str] = None
    force_run_since_start: bool = False
    subject_group_ids: List[str] = Field(
        default_factory=list,
        title="Subject Group IDs",
        description=(
            "List of ER subject-group UUIDs whose members' observations should be included. "
            "Picking a parent group includes its sub-groups' subjects (resolved recursively). "
            "Run the 'show_permissions' action to find UUIDs available to this account. "
            "An empty list applies no group constraint."
        ),
    )

    ui_global_options: GlobalUISchemaOptions = GlobalUISchemaOptions(
        order=["start_datetime", "end_datetime", "subject_group_ids", "force_run_since_start"],
    )


class PullEventsConfig(PullActionConfiguration):
    start_datetime: str
    end_datetime: Optional[str] = None
    force_run_since_start: bool = False
    event_types: List[str] = Field(
        default_factory=list,
        title="Event Types",
        description=(
            "List of ER event-type slugs to pull, e.g. ['wildlife_sighting_rep', 'poacher_sighting_rep']. "
            "Run the 'show_permissions' action to see the slugs available for this account. "
            "Combined with Event Categories using ER's AND semantics. "
            "An empty list applies no event-type constraint."
        ),
    )
    event_categories: List[str] = Field(
        default_factory=list,
        title="Event Categories",
        description=(
            "List of ER event-category slugs, e.g. ['wildlife', 'monitoring']. "
            "ER applies type and category filters with AND semantics. "
            "An empty list applies no category constraint."
        ),
    )

    ui_global_options: GlobalUISchemaOptions = GlobalUISchemaOptions(
        order=["start_datetime", "end_datetime", "event_types", "event_categories", "force_run_since_start"],
    )

