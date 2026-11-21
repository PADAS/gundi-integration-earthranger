"""Shared devices as Gundi sources, so the connection's Sources tab lists them.

Share writes straight to the EarthRanger destinations, so Gundi never sees the
points and never discovers their sources the way it does for routed data. Each
device whose points are shared is registered once on the connection, with the
id Pull Observations gives it (the device's manufacturer id, or er-src-<uuid>)
and the subject's name, so switching between Pull and Share keeps one row per
device. Registering never stops a share: a failure is logged and tried again on
a later run.
"""
import logging
from contextlib import AsyncExitStack
from typing import Awaitable, Callable, Dict, Iterable, List, Optional

import httpx
from gundi_client_v2 import GundiClient
from gundi_client_v2.errors import GundiClientError as _GundiClientError
from redis.exceptions import RedisError

from app.services.errors import IntegrationError
from .state import OBSERVATIONS_ACTION, ShareState

logger = logging.getLogger(__name__)

# Every Gundi client error (sign-in, API answer, token cache setup), and a Redis
# error from a shared token cache: none of them may stop the share.
GundiClientError = (_GundiClientError, RedisError)

# Gundi answers 409 when the connection already has a source with this id.
REGISTERED_STATUSES = {200, 201, 409}


def registered_key(device_id: str) -> str:
    return f"gundi-source:{device_id}"


def external_source_id(device_id: str, device: Optional[dict]) -> str:
    """The id Pull Observations gives a device (app/actions/source_profiles.py)."""
    return (device or {}).get("manufacturer_id") or f"er-src-{device_id}"


class GundiSources:
    """Registers shared devices as sources on one connection, at most once each."""

    def __init__(self, integration_id: str, state: ShareState, stack: AsyncExitStack, client=None):
        self.integration_id = integration_id
        self.state = state
        self._stack = stack
        self._client = client
        # Devices added to Gundi's sources in this run: "<subject> (<external id>)" -> 1.
        self.added: Dict[str, int] = {}

    async def _gundi(self):
        if self._client is None:
            self._client = await self._stack.enter_async_context(GundiClient())
        return self._client

    async def note_shared(self, subject: dict, device_ids: Iterable[str],
                          read_devices: Callable[[], Awaitable[List[dict]]]) -> None:
        try:
            new = [d for d in sorted(set(device_ids))
                   if not await self.state.get_value(OBSERVATIONS_ACTION, registered_key(d))]
        except RedisError as e:
            logger.warning("Couldn't check which shared devices are in Gundi's sources: %s", e)
            return
        if not new:
            return
        label = subject.get("name") or str(subject.get("id"))
        try:
            devices: Dict[str, dict] = {str(s.get("id")): s for s in await read_devices()}
        except IntegrationError as e:
            logger.warning("Couldn't read %s's devices to add them to Gundi's sources: %s", label, e.message)
            return
        try:
            client = await self._gundi()
        except (httpx.HTTPError, *GundiClientError) as e:  # the share itself goes on
            logger.warning("Couldn't reach Gundi to add shared devices to its sources: %s", type(e).__name__)
            return
        for device_id in new:
            payload = {"provider": self.integration_id,
                       "external_id": external_source_id(device_id, devices.get(device_id)),
                       "name": label}
            try:
                response = await client._post(f"{client.api_base_path}/sources/", data=payload)
            except (httpx.HTTPError, *GundiClientError) as e:
                logger.warning("Couldn't add device %s to Gundi's sources: %s", payload["external_id"],
                               type(e).__name__)
                continue
            if response.status_code not in REGISTERED_STATUSES:
                logger.warning("Gundi didn't add device %s to its sources (HTTP %s)", payload["external_id"],
                               response.status_code)
                continue
            try:
                await self.state.set_value(OBSERVATIONS_ACTION, registered_key(device_id), payload["external_id"])
                if response.status_code != 409:  # 409: Gundi already listed it
                    self.added[f"{label} ({payload['external_id']})"] = 1
            except RedisError as e:
                logger.warning("Couldn't record that device %s is in Gundi's sources: %s", device_id, e)
