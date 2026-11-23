"""sync_observations: share subject tracks with each destination (spec §8)."""
import hashlib
import json
import logging
import time
from contextlib import AsyncExitStack
from datetime import timedelta
from typing import Dict, FrozenSet, List, Optional

from gundi_core.events import LogLevel

from app import settings
from app.services.action_scheduler import trigger_action
from app.services.activity_logger import log_action_activity
from app.services.errors import IntegrationError
from .clients import auth_config_of, halts_site, open_site, source_url_of
from .destinations import resolve_destinations
from .geo import load_area
from .gundi_sources import GundiSources
from .modes import observation_settings
from .naming import site_host
from .observations import (
    add_lineage, advance, build_payload, has_valid_location, in_assignment, is_new, note_read, parse_assignments,
    passed_through, point_batches, read_start, OVERLAP, SHORT_OVERLAP,
)
from .reporting import DestinationReport, is_stalled, log_overlap, log_skipped_destinations, names, publish_report, say
from .state import OBSERVATIONS_ACTION, Marker, ShareState
from .subject_cleanup import SUBJECT_CLEANUP_FAILED, cleanup_due, mark_cleaned, remove_deleted_subjects
from .timeutil import parse_dt, utcnow

logger = logging.getLogger(__name__)

BACKFILL = timedelta(hours=12)
SUBJECT_ID_CHUNK = 25
BUDGET_FRACTION = 0.8
LEASE_MARGIN_SECONDS = 30
SUBJECT_CURSOR = "subject-cursor"
PROBE_REJECTIONS = 7  # the batch, its two halves and its four quarters

SOURCE_DENIED = "Some subjects' tracks weren't shared because the source site didn't allow reading them."
SOURCE_FAILED = "Some subjects' tracks weren't shared because the source site couldn't return them."
REJECTED = "Some track points weren't shared because the destination turned them down."
NOT_ACCEPTED = "Some track points weren't shared because the destination couldn't take them."
ALL_REJECTED = "Some track points weren't shared because the destination turned down every point in the batch."
DETAILS_NOT_UPDATED = "Some subjects' details weren't updated because the destination didn't accept the change."
# Detail lines: what a run shared with a destination, or left out and why (INFO).
POINTS_SHARED = say("{name} points shared", "{count} points shared: {names}", with_counts=True)
NO_DEVICE = say("Subject {name} not shared because it has no device assigned",
                "{count} subjects not shared because they have no device assigned: {names}")
NO_NEW_POINTS = say("Subject {name} had no new points to share", "{count} subjects had no new points to share: {names}")
NO_LOCATION = say("1 point not shared because it has no valid location: {name}",
                  "{count} points not shared because they have no valid location: {names}", with_counts=True)
OUTSIDE_ASSIGNMENT = say("1 point not shared because it was recorded outside its device's assignment to the subject: {name}",
                         "{count} points not shared because they were recorded outside their device's assignment "
                         "to the subject: {names}", with_counts=True)
OUTSIDE_AREA = say("1 point not shared because it's outside the area filter: {name}",
                   "{count} points not shared because they're outside the area filter: {names}", with_counts=True)
CAME_FROM_DEST = say("1 point not shared because it came from this destination: {name}",
                     "{count} points not shared because they came from this destination: {names}", with_counts=True)
DETAILS_UPDATED = say("Details for subject {name} and its devices updated to match the source",
                      "Details for {count} subjects and their devices updated to match the source: {names}")


def subjects_shared(source_groups: str, dest_group: str):
    where = f"to subject group {dest_group}" if dest_group else "to the destination"
    return say(f"Subject {{name}} from {_braces(source_groups)} shared {_braces(where)}",
               f"{{count}} subjects from {_braces(source_groups)} shared {_braces(where)}: {{names}}")


def first_share(window: str):
    return say(f"First share for subject {{name}}: tracks {_braces(window)}",
               f"First share for {{count}} subjects: tracks {_braces(window)}: {{names}}")


def _braces(text: str) -> str:
    return text.replace("{", "{{").replace("}", "}}")


def gundi_sources_title(added: Dict[str, int]) -> str:
    count = len(added)
    if count == 1:
        return f"Device {next(iter(added))} added to this route's Sources tab"
    return f"{count} devices added to this route's Sources tab: {names(added, count)}"


# How often subject and device details are compared with what was last sent to each destination.
DETAILS_CHECK_EVERY_SECONDS = 24 * 60 * 60
# A subject is re-read from 12 hours before its marker once an hour, for points that arrive
# late; in the runs in between only from just before the marker.
FULL_REREAD_EVERY_SECONDS = 60 * 60


def subject_marker_key(destination_id: str, subject_id: str) -> str:
    return f"{destination_id}:subject:{subject_id}"


def waiting_key(destination_id: str) -> str:
    return f"{destination_id}:waiting"


async def select_subjects(source, group_ids: FrozenSet[str], group_names: Optional[List[str]] = None) -> List[dict]:
    subjects: Dict[str, dict] = {}
    for group in await source.list_subject_groups():
        if group_ids and str(group.get("id")) not in group_ids:
            continue
        if group_names is not None and group_ids:
            group_names.append(group.get("name") or str(group.get("id")))
        for subject in group.get("subjects") or []:
            subjects.setdefault(str(subject["id"]), subject)
    # A stable order lets a follow-up run resume after the saved cursor.
    return [subjects[k] for k in sorted(subjects)]


async def _load_assignments(source, subject_ids: List[str]):
    """Device assignments by subject, and the subjects whose assignments couldn't be read.
    A chunk that fails on its own is skipped, as v1 skipped the subject; site-wide trouble
    (credentials, reachability, server errors) still stops the run."""
    raw, unread = [], {}
    for i in range(0, len(subject_ids), SUBJECT_ID_CHUNK):
        chunk = subject_ids[i:i + SUBJECT_ID_CHUNK]
        try:
            raw.extend(await source.get_source_assignments(chunk))
        except IntegrationError as e:
            if halts_site(e):
                raise
            unread.update({subject_id: e for subject_id in chunk})
    return parse_assignments(raw), unread


async def run_sync_observations(integration, config) -> dict:
    integration_id = str(integration.id)
    state = ShareState(integration_id)
    if not await state.try_lease(OBSERVATIONS_ACTION, settings.MAX_ACTION_EXECUTION_TIME + LEASE_MARGIN_SECONDS):
        await log_overlap(integration_id, OBSERVATIONS_ACTION, "ER2ER Observation Sharing")
        return {"status": "skipped", "reason": "Another sync_observations run is still in progress."}
    try:
        result, needs_more_time = await _run(integration, config, state)
    finally:
        await state.release_lease(OBSERVATIONS_ACTION)
    if needs_more_time:
        try:
            await trigger_action(integration_id, OBSERVATIONS_ACTION)
        except Exception as e:
            # The points sent and their markers are saved; the next scheduled run carries on from there.
            logger.warning("sync_observations: couldn't start a follow-up run (%s)", type(e).__name__)
    return result


async def _run(integration, config, state: ShareState):
    integration_id = str(integration.id)
    deadline = time.monotonic() + settings.MAX_ACTION_EXECUTION_TIME * BUDGET_FRACTION
    auth = auth_config_of(integration.configurations)
    source_url = source_url_of(integration, auth)
    share = observation_settings(source_url, config)
    destinations, skipped = await resolve_destinations(integration_id, source_url)
    await log_skipped_destinations(integration_id, OBSERVATIONS_ACTION, state, skipped)
    if not destinations:
        await log_action_activity(integration_id=integration_id, action_id=OBSERVATIONS_ACTION,
                                  title="No EarthRanger destinations to share subject tracks with.",
                                  level=LogLevel.WARNING)
        return {"destinations": []}, False

    reports = {d.id: DestinationReport(destination=d.name, kind="Tracks") for d in destinations}
    gundi_sources = None
    dest_group = share.destination_subject_group or ""
    provider_keys = {d.id: d.provider_key for d in destinations if d.provider_key}
    freshest: Dict[str, Marker] = {}  # per destination, the subject marker that moved most recently
    try:
        async with AsyncExitStack() as stack:
            source = await stack.enter_async_context(
                open_site(source_url, auth, role="source"))
            sites = {}
            for d in destinations:
                try:
                    sites[d.id] = await stack.enter_async_context(open_site(d.url, d.auth, role="destination"))
                except IntegrationError as e:
                    reports[d.id].error(e.message)
            area = await load_area(source, share.area_feature_groups, polygons_only=True)
            gundi_sources = GundiSources(integration_id, state, stack)
            group_names: List[str] = []
            subjects = await select_subjects(source, share.subject_groups, group_names)
            source_groups = (f"subject group{'s' if len(group_names) > 1 else ''} {', '.join(group_names)}"
                             if group_names else "all subject groups")
            subjects_title = subjects_shared(source_groups, share.destination_subject_group or "")
            assignments, unread = await _load_assignments(source, [str(s["id"]) for s in subjects])
            src_user = share.src_user or (await source.get_me()).get("username", "")
            now = utcnow()
            prefix = f"site:{site_host(source.url)}#user:{src_user}"
            cursor = await state.get_cursor(OBSERVATIONS_ACTION, SUBJECT_CURSOR)
            last = cursor
            for subject in subjects:
                subject_id = str(subject["id"])
                if cursor is not None and subject_id <= cursor:
                    continue
                if not sites:
                    break
                if subject_id in unread:
                    _source_failed(unread[subject_id], sites, reports, subject.get("name") or subject_id)
                    last = subject_id
                    continue
                finished = time.monotonic() <= deadline and await _share_subject(
                    source, sites, reports, freshest, state, share, area, assignments, subject, prefix, now,
                    deadline, provider_keys, gundi_sources, subjects_title)
                if not finished:
                    # Out of time: the next run starts again at this subject; its sent batches
                    # already moved its markers, so nothing is sent twice.
                    for d_id in sites:
                        reports[d_id].timed_out = True
                    if last is not None:
                        await state.set_cursor(OBSERVATIONS_ACTION, SUBJECT_CURSOR, last)
                    else:
                        await state.delete_marker(OBSERVATIONS_ACTION, SUBJECT_CURSOR)
                    break
                last = subject_id
            else:
                await state.delete_marker(OBSERVATIONS_ACTION, SUBJECT_CURSOR)
            if share.remove_deleted_subjects:
                await _remove_deleted_subjects(state, source, sites, reports, dest_group,
                                               provider_keys, share.provider_key, deadline)
    finally:
        if gundi_sources is not None and gundi_sources.added:
            try:
                await log_action_activity(integration_id=integration_id, action_id=OBSERVATIONS_ACTION,
                                          title=gundi_sources_title(gundi_sources.added), level=LogLevel.INFO,
                                          data={"count": len(gundi_sources.added), "items": gundi_sources.added})
            except Exception:
                logger.exception("Couldn't log the devices added to Gundi's sources")
        # Published even when a source read stops the run, so points already sent are reported.
        for d in destinations:
            report = reports[d.id]
            try:
                _fold_subjects(report, dest_group)
                try:
                    await _drop_known_subjects(state, d.id, report)
                except Exception:
                    logger.exception("Couldn't check which subjects were already named for %s", d.name)
                await publish_report(integration_id, OBSERVATIONS_ACTION, report, state=state, destination_id=d.id)
                marker = await _progress_marker(state, d.id, freshest.get(d.id), report)
                if report.errors and is_stalled(marker, utcnow()):
                    await log_action_activity(
                        integration_id=integration_id, action_id=OBSERVATIONS_ACTION, level=LogLevel.WARNING,
                        title=f"{report.heading}: no progress since {marker.advanced_at:%Y-%m-%d %H:%M} UTC. "
                              f"Check the errors above.",
                    )
            except Exception:
                logger.exception("Couldn't publish the tracks report for %s", d.name)
    return ({"destinations": [r.as_dict() for r in reports.values()]},
            any(r.timed_out for r in reports.values()))


async def _remove_deleted_subjects(state: ShareState, source, sites, reports, dest_group: str,
                                   provider_keys: Dict[str, str], default_key: str, deadline: float) -> None:
    """Once a day per destination, with the time the sharing left over."""
    now = utcnow()
    site_stamp = f"site:{site_host(source.url)}#"
    for d_id, dest in list(sites.items()):
        if time.monotonic() > deadline or not await cleanup_due(state, d_id, now):
            continue
        try:
            finished = await remove_deleted_subjects(
                source, dest, dest_group, provider_keys.get(d_id) or default_key, site_stamp, reports[d_id],
                lambda: time.monotonic() > deadline, state, d_id)
        except IntegrationError as e:
            # Tried again tomorrow rather than on every run, so the error is logged once a day.
            reports[d_id].error(f"{SUBJECT_CLEANUP_FAILED}: {e.message}")
            await mark_cleaned(state, d_id, now)
            continue
        if finished:
            await mark_cleaned(state, d_id, now)


async def _progress_marker(state: ShareState, destination_id: str, freshest, report: DestinationReport):
    """The marker the stall check reads. A destination that hasn't taken a point yet
    has no subject markers, so the time its errors started stands in for one."""
    key = waiting_key(destination_id)
    if freshest is not None or not report.errors:
        await state.delete_marker(OBSERVATIONS_ACTION, key)
        return freshest
    waiting = await state.get_marker(OBSERVATIONS_ACTION, key)
    if waiting is None:
        now = utcnow()
        waiting = Marker(since=now, advanced_at=now)
        await state.set_marker(OBSERVATIONS_ACTION, key, waiting)
    return waiting


async def _share_subject(source, sites, reports, freshest, state, share, area, assignments, subject, prefix, now,
                         deadline: float = float("inf"), provider_keys: Optional[Dict[str, str]] = None,
                         gundi_sources: Optional[GundiSources] = None, subjects_title=None) -> bool:
    """Share one subject's new points with every destination. False when the run ran out
    of time part-way, so the caller resumes at this subject next time."""
    subject_id = str(subject["id"])
    label = subject.get("name") or subject_id
    markers = {d_id: await state.get_marker(OBSERVATIONS_ACTION, subject_marker_key(d_id, subject_id))
               for d_id in sites}
    backfill_from = now - BACKFILL
    full_reread = await state.once_per(OBSERVATIONS_ACTION, f"reread:{subject_id}", FULL_REREAD_EVERY_SECONDS)
    start = read_start(list(markers.values()),
                       max(parse_dt(share.start_from), backfill_from) if share.start_from else backfill_from,
                       OVERLAP if full_reread else SHORT_OVERLAP)
    try:
        observations = await source.get_observations(subject_id, start)
    except IntegrationError as e:
        return _source_failed(e, sites, reports, label)

    candidates = sorted(
        (o for o in observations
         if has_valid_location(o)
         and in_assignment(o, assignments.get(subject_id, []))
         and (area is None or area.contains_point(o["location"]["longitude"], o["location"]["latitude"]))),
        key=lambda o: parse_dt(o["recorded_at"]),
    )
    to_send = {d_id: [o for o in candidates if is_new(o, markers[d_id], full_reread) and not passed_through(o, site_host(dest.url))]
               for d_id, dest in sites.items()}
    subject_assignments = assignments.get(subject_id, [])
    for d_id, dest in sites.items():
        _note_left_out(reports[d_id], label, observations, markers[d_id], full_reread, subject_assignments, area,
                       site_host(dest.url))
        if markers[d_id] is None and to_send[d_id]:
            first = max(parse_dt(share.start_from), backfill_from) if share.start_from else backfill_from
            window = f"from {first:%Y-%m-%d %H:%M} UTC"
            if first == backfill_from:
                window += f" (the last {int(BACKFILL.total_seconds() // 3600)} hours)"
            reports[d_id].detail("first-share", first_share(window), item=label)
    details_key = {d_id: f"{d_id}:details-check:{subject_id}" for d_id in sites}
    details_due = {d_id: share.subject_additional and markers[d_id] is not None
                   and await state.is_due(OBSERVATIONS_ACTION, details_key[d_id]) for d_id in sites}
    source_additional = {}
    # The subject's devices are read only when something will use them.
    if share.subject_additional and (any(to_send.values()) or any(details_due.values())):
        try:
            source_additional = {str(s.get("id")): s.get("additional") or {}
                                 for s in await source.get_subject_sources(subject_id)}
        except IntegrationError as e:
            return _source_failed(e, sites, reports, label)

    fingerprint = f"{prefix}#subject:{subject_id}#rts:{now:%Y%m%d%H%M%S}"
    finished = True
    shared_devices = set()  # devices whose points reached a destination this run
    for d_id, dest in list(sites.items()):
        key = subject_marker_key(d_id, subject_id)
        marker = markers[d_id]
        complete = True
        for batch in point_batches(to_send[d_id]):
            if time.monotonic() > deadline:
                complete = finished = False
                break
            payloads = [add_lineage(build_payload(subject, o, share, source_additional), fingerprint) for o in batch]
            sent_before = reports[d_id].created
            try:
                # The route's field mapping for this destination wins over the share's own setting.
                provider_key = (provider_keys or {}).get(d_id) or share.provider_key
                complete = await _send(dest, provider_key, payloads, reports[d_id], label)
            except IntegrationError as e:
                reports[d_id].error(e.message)
                del sites[d_id]
                complete = False
            if not complete:
                break
            sent = reports[d_id].created - sent_before
            if sent:
                reports[d_id].detail("points", POINTS_SHARED, item=label, count=sent)
                if subjects_title is not None and label not in _items(reports[d_id], "subjects"):
                    reports[d_id].detail("subjects", subjects_title, item=label)
            shared_devices.update(str(o["source"]) for o in batch if o.get("source"))
            marker = advance(marker, batch, now, keep_created=True)
            await state.set_marker(OBSERVATIONS_ACTION, key, marker)
        if complete and observations:
            # Points that will never be shared (filtered out) still move the marker, so they aren't re-read.
            moved = note_read(advance(marker, observations, now), now, start, full_reread)
            if moved != marker:
                marker = moved
                await state.set_marker(OBSERVATIONS_ACTION, key, marker)
        if details_due[d_id] and d_id in sites:
            try:
                checked = await _refresh_details(dest, d_id, state, subject, source_additional, reports[d_id], label)
            except IntegrationError as e:
                # Like a failed point send: this destination stops for the run, the others go on.
                reports[d_id].error(e.message)
                del sites[d_id]
                checked = False
            if checked:  # the daily gate is taken only once the check is done
                await state.once_per(OBSERVATIONS_ACTION, details_key[d_id], DETAILS_CHECK_EVERY_SECONDS)
        if marker is not None:
            current = reports[d_id].marker
            reports[d_id].marker = marker.since if current is None else max(current, marker.since)
            if d_id not in freshest or marker.advanced_at > freshest[d_id].advanced_at:
                freshest[d_id] = marker
    if gundi_sources is not None and shared_devices:
        # Share bypasses Gundi's routing, so Gundi only lists these devices if told.
        await gundi_sources.note_shared(subject, shared_devices, lambda: source.get_subject_sources(subject_id))
    return finished


def _note_left_out(report: DestinationReport, label: str, observations: List[dict], marker, full_reread: bool,
                   subject_assignments, area, dest_host: str) -> None:
    """Say why this subject's new points (not seen by an earlier run) weren't shared."""
    if not subject_assignments:
        report.detail("no-device", NO_DEVICE, item=label)
        return
    fresh = [o for o in observations if is_new(o, marker, full_reread)]
    left_out = {"no-location": 0, "outside-assignment": 0, "outside-area": 0, "came-from-dest": 0}
    shared = 0
    for o in fresh:
        if not has_valid_location(o):
            left_out["no-location"] += 1
        elif not in_assignment(o, subject_assignments):
            left_out["outside-assignment"] += 1
        elif area is not None and not area.contains_point(o["location"]["longitude"], o["location"]["latitude"]):
            left_out["outside-area"] += 1
        elif passed_through(o, dest_host):
            left_out["came-from-dest"] += 1
        else:
            shared += 1
    titles = {"no-location": NO_LOCATION, "outside-assignment": OUTSIDE_ASSIGNMENT, "outside-area": OUTSIDE_AREA,
              "came-from-dest": CAME_FROM_DEST}
    for key, n in left_out.items():
        if n:
            report.detail(key, titles[key], item=label, count=n)
    if not shared and not any(left_out.values()):
        report.detail("no-new-points", NO_NEW_POINTS, item=label)


def _fold_subjects(report: DestinationReport, dest_group: str) -> None:
    """The summary says how many subjects were shared and where. One detail line then
    names them: the points per subject when those differ, otherwise just the subjects."""
    subjects = _items(report, "subjects")
    if not subjects:
        return
    where = f" to subject group {dest_group}" if dest_group else ""
    report.summary_extra = f" from {len(subjects):,} subject{'s' if len(subjects) != 1 else ''}{where}"
    points = _items(report, "points")
    if len(set(points.values())) <= 1:
        report.details.pop("points", None)
    else:
        report.details.pop("subjects", None)


async def _drop_known_subjects(state: ShareState, destination_id: str, report: DestinationReport) -> None:
    """The line naming the shared subjects appears only when a subject joins the list,
    not on every run that shares the same subjects again."""
    subjects = _items(report, "subjects")
    if not subjects:
        return
    logged = set(await state.get_logged_subjects(destination_id))
    if set(subjects) <= logged:
        report.details.pop("subjects", None)
        return
    await state.set_logged_subjects(destination_id, list(logged | set(subjects)))


def _items(report: DestinationReport, key: str) -> Dict[str, int]:
    group = report.details.get(key)
    return group.items if group else {}


def _source_failed(error: IntegrationError, sites, reports, label: str) -> bool:
    denied = error.status_code == 403
    if halts_site(error) and not denied:
        raise error
    for d_id in sites:
        reports[d_id].error(SOURCE_DENIED if denied else SOURCE_FAILED, item=f"{label}: {error.message}")
    return True


class _WholeBatchRejected(Exception):
    """The batch, both halves and all four quarters were turned down, so the batch itself is the problem."""


class _Split:
    def __init__(self):
        self.rejected: List[tuple] = []
        self.rejection = ""  # the destination's message for the latest 400


async def _send(dest, provider_key: str, payloads: List[dict], report: DestinationReport, label: str) -> bool:
    """Post points, halving a rejected batch until each bad point is found and skipped.
    False when the destination didn't take them for another reason, or turned down
    the batch as a whole (every point, or the batch, its halves and its quarters),
    so the marker holds. Raises the errors that stop the destination for this run."""
    split = _Split()
    try:
        if len(payloads) >= 4:
            complete = await _send_probing(dest, provider_key, payloads, report, label, split)
        else:
            complete = await _send_splitting(dest, provider_key, payloads, report, label, split)
    except _WholeBatchRejected as whole:
        report.failed += len(payloads)
        report.error(ALL_REJECTED, item=f"{label}: {whole}")
        return False
    except IntegrationError:
        _report_rejected(report, label, split.rejected)
        raise
    if complete and len(payloads) > 1 and len(split.rejected) == len(payloads):
        report.failed += len(payloads)
        report.error(ALL_REJECTED, item=f"{label}: {split.rejected[0][1]}")
        return False
    _report_rejected(report, label, split.rejected)
    return complete


def _report_rejected(report: DestinationReport, label: str, rejected: List[tuple]):
    for recorded_at, message in rejected:
        report.failed += 1
        report.error(REJECTED, item=f"{label} at {recorded_at}: {message}")


async def _post(dest, provider_key: str, payloads: List[dict], report: DestinationReport, label: str,
                split: _Split):
    """True when taken, None when turned down (400), False when not taken for another reason."""
    try:
        await dest.post_sharing_status(provider_key, payloads)
        report.created += len(payloads)
        return True
    except IntegrationError as e:
        if halts_site(e):
            raise
        if e.status_code != 400:
            report.failed += len(payloads)
            report.error(NOT_ACCEPTED, item=f"{label}: {e.message}")
            return False
        split.rejection = e.message
        return None


async def _send_probing(dest, provider_key: str, payloads: List[dict], report: DestinationReport, label: str,
                        split: _Split) -> bool:
    """Tries the batch, its halves and its quarters before descending, so a batch-level
    rejection costs at most seven posts instead of a full split."""
    outcome = await _post(dest, provider_key, payloads, report, label, split)
    if outcome is not None:
        return outcome
    batch_message = split.rejection
    rejected_nodes, pending = 1, []
    middle = len(payloads) // 2
    for half in (payloads[:middle], payloads[middle:]):
        outcome = await _post(dest, provider_key, half, report, label, split)
        if outcome is True:
            continue
        if outcome is False:
            return False
        rejected_nodes += 1
        quarter_at = len(half) // 2
        for quarter in (half[:quarter_at], half[quarter_at:]):
            outcome = await _post(dest, provider_key, quarter, report, label, split)
            if outcome is True:
                continue
            if outcome is False:
                return False
            rejected_nodes += 1
            pending.append((quarter, split.rejection))
    if rejected_nodes == PROBE_REJECTIONS:
        raise _WholeBatchRejected(batch_message)
    for quarter, message in pending:
        if not await _split_rejected(dest, provider_key, quarter, report, label, split, message):
            return False
    return True


async def _split_rejected(dest, provider_key, payloads, report, label, split, message) -> bool:
    """Split a batch already known to be turned down."""
    if len(payloads) == 1:
        split.rejected.append((payloads[0]["recorded_at"], message))
        return True
    middle = len(payloads) // 2
    return (await _send_splitting(dest, provider_key, payloads[:middle], report, label, split)
            and await _send_splitting(dest, provider_key, payloads[middle:], report, label, split))


async def _send_splitting(dest, provider_key: str, payloads: List[dict], report: DestinationReport, label: str,
                          split: _Split) -> bool:
    outcome = await _post(dest, provider_key, payloads, report, label, split)
    if outcome is not None:
        return outcome
    return await _split_rejected(dest, provider_key, payloads, report, label, split, split.rejection)


async def _refresh_details(dest, destination_id: str, state, subject: dict, source_additional: Dict[str, dict],
                           report: DestinationReport, label: str):
    """EarthRanger sets a shared subject's details only when it first creates it, so
    details changed at the source later are sent here, once they differ from what this
    destination last got. The copy keeps the source subject's id, and its device keeps
    the source device's id as its manufacturer id."""
    subject_id = str(subject["id"])
    subject_details = subject.get("additional") or {}
    digest = hashlib.sha1(json.dumps({"subject": subject_details, "sources": source_additional},
                                     sort_keys=True, default=str).encode()).hexdigest()
    key = f"{destination_id}:details:{subject_id}"
    stored = await state.get_value(OBSERVATIONS_ACTION, key)
    if stored == digest:
        return True
    if stored is None:
        # First check for this subject at this destination: what it has now is taken as the
        # starting point, so values a destination admin set aren't overwritten at cutover.
        await state.set_value(OBSERVATIONS_ACTION, key, digest)
        return True
    try:
        await dest.patch_subject(subject_id, {"additional": subject_details})
        for copy in await dest.get_subject_sources(subject_id):
            details = source_additional.get(str(copy.get("manufacturer_id")))
            if details is not None and (copy.get("additional") or {}) != details:
                await dest.patch_source(str(copy["id"]), {"additional": details})
        report.detail("details-updated", DETAILS_UPDATED, item=label)
    except IntegrationError as e:
        if e.status_code == 404:
            return False  # not at the destination yet; checked again next run
        if halts_site(e) and e.status_code != 403:
            raise
        report.error(DETAILS_NOT_UPDATED, item=f"{label}: {e.message}", level=LogLevel.WARNING)
        return True  # turned down: checked again tomorrow, not every run
    await state.set_value(OBSERVATIONS_ACTION, key, digest)
    return True
