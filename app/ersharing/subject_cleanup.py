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
DEVICE_NOT_CHECKED = "Some subjects weren't checked for removal because the source site couldn't show their devices."
SUBJECT_CLEANUP_FAILED = "The daily check for subjects deleted at the source didn't finish"


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
                                  report: DestinationReport, out_of_time: Callable[[], bool],
                                  state: ShareState, destination_id: str) -> bool:
    """Removes the subjects in the destination group that this share created and whose
    devices are all gone at the source. Returns True once every subject was checked;
    an unfinished pass carries on after the last subject checked.

    A subject counts as this share's only when all its devices came in under the share's
    provider key and its latest point was last shared from this source site, so subjects
    other sites share into the same group are left alone."""
    cursor_key = f"{cleanup_key(destination_id)}:cursor"
    after = await state.get_cursor(OBSERVATIONS_ACTION, cursor_key)
    groups = [g for g in await dest.get_subject_groups(flat=True) if g.get("name") == group_name]
    subjects = sorted({str(x["id"]): x for g in groups for x in g.get("subjects") or []}.items())
    for subject_id, subject in subjects:
        if after is not None and subject_id <= after:
            continue
        if out_of_time():
            return False
        if not await _check_subject(source, dest, subject, provider_key, site_stamp, report):
            break  # the source can't show its devices now: tried again tomorrow
        await state.set_cursor(OBSERVATIONS_ACTION, cursor_key, subject_id)
    await state.delete_marker(OBSERVATIONS_ACTION, cursor_key)
    return True


async def _check_subject(source, dest, subject: dict, provider_key: str, site_stamp: str,
                         report: DestinationReport) -> bool:
    """False when the source can't be asked about devices any more today."""
    subject_id = str(subject["id"])
    name = subject.get("name") or subject_id
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
            # A device the source can't show is kept: only a device the source says is gone counts.
            title = NOT_CHECKED if e.status_code == 403 else DEVICE_NOT_CHECKED
            report.error(title, item=f"{name}: {e.message}", level=LogLevel.WARNING)
            return not (e.status_code == 403 or halts_site(e))
    try:
        await dest.delete_subject(subject_id)
    except IntegrationError as e:
        if halts_site(e):
            raise
        if e.status_code != 404:
            report.error(NOT_REMOVED, item=f"{name}: {e.message}")
            return True
    report.deleted += 1
    report.detail("subjects-removed", SUBJECTS_REMOVED, item=name)
    return True
