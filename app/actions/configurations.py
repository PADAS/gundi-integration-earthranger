from datetime import datetime
from enum import Enum
from typing import List, Optional

from pydantic import Field

from .core import AuthActionConfiguration, PullActionConfiguration, ExecutableActionMixin


class AuthenticateConfig(AuthActionConfiguration, ExecutableActionMixin):
    username: Optional[str] = Field(
        "",
        example="user@pamdas.org",
        description="Username used to authenticate against Earth Ranger API",
    )
    password: Optional[str] = Field(
        "",
        example="passwd1234abc",
        description="Password used to authenticate against Earth Ranger API",
    )
    token: Optional[str] = Field(
        "",
        example="1b4c1e9c-5ee0-44db-c7f1-177ede2f854a",
        description="Token used to authenticate against Earth Ranger API",
    )


class PullObservationsConfig(PullActionConfiguration):
    start_datetime: str
    end_datetime: Optional[str] = None
    force_run_since_start: bool = False


class PullEventsConfig(PullActionConfiguration):
    start_datetime: str
    end_datetime: Optional[str] = None
    force_run_since_start: bool = False
