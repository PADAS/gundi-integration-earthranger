"""Daily removal of destination subjects whose devices were deleted at the source,
the subjects' counterpart of removing event copies (sync_events.reconcile_destination)."""
from datetime import datetime, timedelta
from typing import Callable, List, Optional

from gundi_core.events import LogLevel

from app.services.errors import IntegrationError
from .clients import halts_site
from .reporting import DestinationReport, say
from .state import OBSERVATIONS_ACTION, Marker, ShareState

CLEANUP_EVERY = timedelta(hours=24)

SUBJECTS_REMOVED = say("Subject {name} removed because its device was deleted at the source",
                       "{count} subjects removed because their devices were deleted at the source: {names}")
NOT_CHECKED = ("Some subjects weren't checked for removal because the source site didn't allow "
               "reading their devices.")
NOT_REMOVED = "Couldn't remove a subject whose device was deleted at the source"


def cleanup_key(destination_id: str) -> str:
    return f"{destination_id}:subject-cleanup"


async def cleanup_due(state: ShareState, destination_id: str, now: datetime) -> bool:
    marker = await state.get_marker(OBSERVATIONS_ACTION, cleanup_key(destination_id))
    return marker is None or now - marker.since >= CLEANUP_EVERY


async def mark_cleaned(state: ShareState, destination_id: str, now: datetime) -> None:
    await state.set_marker(OBSERVATIONS_ACTION, cleanup_key(destination_id), Marker(since=now, advanced_at=now))


def _lineage(observation: Optional[dict]) -> List[str]:
    details = (observation or {}).get("observation_details") or {}
    return [entry for entry in details.get("sharing_lineage") or [] if isinstance(entry, str)]


async def remove_deleted_subjects(source, dest, group_name: str, provider_key: str, site_stamp: str,
                                  report: DestinationReport, out_of_time: Callable[[], bool]) -> bool:
    """Removes the subjects in the destination group that this share created and whose
    devices are all gone at the source. Returns True once every subject was checked.

    A subject counts as this share's only when all its devices came in under the share's
    provider key and its latest point was last shared from this source site, so subjects
    other sites share into the same group are left alone."""
    groups = [g for g in await dest.get_subject_groups(flat=True) if g.get("name") == group_name]
    for group in groups:
        for subject in group.get("subjects") or []:
            if out_of_time():
                return False
            if not await _check_subject(source, dest, subject, provider_key, site_stamp, report):
                return True  # the source refused to show its devices: nothing more can be checked today
    return True


async def _check_subject(source, dest, subject: dict, provider_key: str, site_stamp: str,
                         report: DestinationReport) -> bool:
    subject_id = str(subject["id"])
    devices = await dest.get_subject_sources(subject_id)
    if not devices or any(d.get("provider") != provider_key or not d.get("manufacturer_id") for d in devices):
        return True
    lineage = _lineage(await dest.get_latest_observation(subject_id))
    if not lineage or not lineage[-1].lower().startswith(site_stamp.lower()):
        return True
    for device in devices:
        try:
            if await source.get_source(str(device["manufacturer_id"])) is not None:
                return True
        except IntegrationError as e:
            if e.status_code != 403:
                raise
            report.error(NOT_CHECKED, item=f"{subject.get('name') or subject_id}: {e.message}",
                         level=LogLevel.WARNING)
            return False
    try:
        await dest.delete_subject(subject_id)
    except IntegrationError as e:
        if halts_site(e):
            raise
        if e.status_code != 404:
            report.error(NOT_REMOVED, item=f"{subject.get('name') or subject_id}: {e.message}")
            return True
    report.deleted += 1
    report.detail("subjects-removed", SUBJECTS_REMOVED, item=subject.get("name") or subject_id)
    return True
