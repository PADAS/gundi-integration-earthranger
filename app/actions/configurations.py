from datetime import datetime
from enum import Enum
from typing import List, Optional

from pydantic import Field, SecretStr

from .core import AuthActionConfiguration, PullActionConfiguration, ExecutableActionMixin


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
    token: Optional[str] = Field(
        "",
        example="1b4c1e9c-5ee0-44db-c7f1-177ede2f854a",
        description="Token used to authenticate against Earth Ranger API",
    )

    class Config:
        schema_extra = {
            "if": {
                "properties": {"authentication_type": {"const": "token"}}
            },
            "then": {
                "required": ["token"],
                "properties": {
                    "token": {"type": "string", "description": "Token is required if authentication_type is 'token'."}
                },
            },
            "else": {
                "required": ["username", "password"],
                "properties": {
                    "username": {"type": "string",
                                 "description": "Username is required if authentication_type is 'username_password'."},
                    "password": {"type": "string", "format": "password",
                                 "description": "Password is required if authentication_type is 'username_password'."},
                },
            },
        }

print(AuthenticateConfig.schema_json(indent=2))

class PullObservationsConfig(PullActionConfiguration):
    start_datetime: str
    end_datetime: Optional[str] = None
    force_run_since_start: bool = False


class PullEventsConfig(PullActionConfiguration):
    start_datetime: str
    end_datetime: Optional[str] = None
    force_run_since_start: bool = False
