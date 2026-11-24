"""What admins see in the activity log for each run and destination (spec §9)."""
import hashlib
import logging
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta
from typing import Callable, Dict, List, Optional

from gundi_core.events import LogLevel

from app.services.activity_logger import log_action_activity
from .state import Marker

logger = logging.getLogger(__name__)

STALL_AFTER = timedelta(hours=6)
# How often the same "destination skipped" line is repeated in the activity log.
SKIP_LOG_EVERY = timedelta(hours=6)
MAX_EXAMPLES = 5
# Log titles name the action, as the portal does.
ACTION_TITLES = {"Events": "ER2ER Event Sharing", "Tracks": "ER2ER Observation Sharing"}
# A run that changed nothing and had nothing to report logs its summary at most this often.
QUIET_SUMMARY_EVERY = timedelta(hours=24)
# Detail lines that don't stop a run from counting as quiet.
QUIET_DETAILS = {"no-new-points"}
# How many names a detail line lists in its title; its data holds up to MAX_DETAIL_ITEMS.
NAMES_IN_TITLE = 5
MAX_DETAIL_ITEMS = 100


def names(items: Dict[str, int], count: int, with_counts: bool = False) -> str:
    """'a, b, c and 4 more' (with counts: 'a 3, b 1 and 4 more')."""
    shown = list(items.items())[:NAMES_IN_TITLE]
    text = ", ".join(f"{name} {n:,}" if with_counts else name for name, n in shown)
    rest = (len(items) if with_counts else count) - len(shown)
    return f"{text} and {rest:,} more" if rest > 0 else text


def say(single: str, multi: str, with_counts: bool = False) -> Callable[[int, Dict[str, int]], str]:
    """A detail line's title: `single` (with {name}) for one item, `multi` (with {count}
    and {names}) for several."""
    def title(count: int, items: Dict[str, int]) -> str:
        if count == 1 and len(items) == 1:
            return single.format(name=next(iter(items)))
        return multi.format(count=f"{count:,}", names=names(items, count, with_counts))
    return title


@dataclass
class DetailGroup:
    title: Callable[[int, Dict[str, int]], str]
    count: int = 0
    items: Dict[str, int] = field(default_factory=dict)  # name -> how many of the count it covers


@dataclass
class ErrorGroup:
    title: str
    level: LogLevel
    count: int = 0
    examples: List[str] = field(default_factory=list)


@dataclass
class DestinationReport:
    destination: str
    kind: str  # "Events" or "Tracks"
    created: int = 0
    updated: int = 0
    deleted: int = 0
    skipped: int = 0
    failed: int = 0
    marker: Optional[datetime] = None
    timed_out: bool = False
    errors: Dict[str, ErrorGroup] = field(default_factory=dict)
    # What the run did or left out, one INFO line each, in the order first noted.
    details: Dict[str, DetailGroup] = field(default_factory=dict)
    # Added after the counts in the summary (Tracks: which subjects went where).
    summary_extra: str = ""
    # Incident links that failed, by (copy, incident): reported only if still missing at the end.
    unlinked: Dict[tuple, tuple] = field(default_factory=dict)  # -> (event label, reason)

    @property
    def heading(self) -> str:
        return f"{ACTION_TITLES.get(self.kind, self.kind)} → {self.destination}"

    def is_quiet(self) -> bool:
        """Nothing changed, nothing went wrong and nothing was left out."""
        return (not (self.created or self.updated or self.deleted or self.skipped or self.failed)
                and not self.errors and not self.timed_out and set(self.details) <= QUIET_DETAILS)

    def detail(self, key: str, title: Callable[[int, Dict[str, int]], str], item: Optional[str] = None,
               count: int = 1):
        group = self.details.setdefault(key, DetailGroup(title=title))
        group.count += count
        if item is not None and (item in group.items or len(group.items) < MAX_DETAIL_ITEMS):
            group.items[item] = group.items.get(item, 0) + count

    def error(self, title: str, item: Optional[str] = None, level: LogLevel = LogLevel.ERROR):
        group = self.errors.setdefault(title, ErrorGroup(title=title, level=level))
        group.count += 1
        if item is not None and len(group.examples) < MAX_EXAMPLES:
            group.examples.append(item)

    def summary_title(self) -> str:
        if self.is_quiet():
            title = f"{self.heading}: nothing new to share"
            if self.marker:
                title += f"; synced up to {self.marker:%Y-%m-%d %H:%M} UTC"
            return title
        if self.kind == "Tracks":
            counts = f"{self.created:,} points shared{self.summary_extra}, {self.failed} failed"
            if self.deleted:
                counts += f", {self.deleted:,} subject{'s' if self.deleted != 1 else ''} removed"
        else:
            counts = (f"{self.created} created, {self.updated} updated, {self.deleted} deleted, "
                      f"{self.skipped} skipped, {self.failed} failed")
        title = f"{self.heading}: {counts}"
        if self.marker:
            title += f"; synced up to {self.marker:%Y-%m-%d %H:%M} UTC"
        if self.timed_out:
            title += "; stopped at the time limit before finishing"
        return title

    def as_dict(self) -> dict:
        details, unlinked, self.details, self.unlinked = self.details, self.unlinked, {}, {}
        try:
            data = asdict(self)
        finally:
            self.details, self.unlinked = details, unlinked
        del data["unlinked"]
        data["details"] = {k: {"count": g.count, "items": g.items} for k, g in details.items()}
        data["marker"] = self.marker.isoformat() if self.marker else None
        data["errors"] = {k: {"count": g.count, "examples": g.examples} for k, g in self.errors.items()}
        return data


def _group_data(group: ErrorGroup) -> dict:
    """Like the runner's other error logs, the data names what it holds: an error
    group lists its errors, a warning group the items it is about."""
    if group.level == LogLevel.ERROR:
        return {"error_count": group.count, "errors": group.examples}
    return {"count": group.count, "examples": group.examples}


async def publish_report(integration_id: str, action_id: str, report: DestinationReport, state=None,
                         destination_id: Optional[str] = None):
    """Log the run's summary, detail lines and problems for one destination. With `state`,
    a quiet run's summary is logged at most once per QUIET_SUMMARY_EVERY."""
    if report.is_quiet() and state is not None and destination_id is not None:
        if not await state.once_per(action_id, f"{destination_id}:quiet-summary",
                                    int(QUIET_SUMMARY_EVERY.total_seconds())):
            return
    level = LogLevel.WARNING if report.failed or report.errors else LogLevel.INFO
    await log_action_activity(integration_id=integration_id, action_id=action_id,
                              title=report.summary_title(), level=level, data=report.as_dict())
    for group in report.details.values():
        await log_action_activity(integration_id=integration_id, action_id=action_id,
                                  title=f"{report.heading}: {group.title(group.count, group.items)}",
                                  level=LogLevel.INFO, data={"count": group.count, "items": group.items})
    for group in report.errors.values():
        # The portal shows a line of an error-level log in red only when its text
        # mentions an error, so error groups say so up front.
        prefix = "Error: " if group.level == LogLevel.ERROR else ""
        await log_action_activity(integration_id=integration_id, action_id=action_id,
                                  title=f"{prefix}{report.heading}: {group.title}",
                                  level=group.level, data=_group_data(group))


async def log_overlap(integration_id: str, action_id: str, action_title: str) -> None:
    """Says why a run stopped right after it started."""
    try:
        await log_action_activity(integration_id=integration_id, action_id=action_id, level=LogLevel.INFO,
                                  title=f"{action_title} didn't run this time because its previous run "
                                        f"was still going.")
    except Exception:
        logger.exception("Couldn't log the skipped %s run", action_id)


async def log_skipped_destinations(integration_id: str, action_id: str, state, skipped) -> None:
    """Say why each destination was skipped, at most once per SKIP_LOG_EVERY for the
    same destination and reason, so a share that runs every few minutes doesn't
    repeat the same line all day. A new reason is logged straight away."""
    for item in skipped:
        reason_key = hashlib.sha1(item.reason.encode()).hexdigest()[:12]
        if not await state.once_per(action_id, f"skipped:{item.id}:{reason_key}",
                                    int(SKIP_LOG_EVERY.total_seconds())):
            continue
        await log_action_activity(integration_id=integration_id, action_id=action_id,
                                  title=f"Destination {item.name} skipped: {item.reason}", level=LogLevel.WARNING)


def is_stalled(marker: Optional[Marker], now: datetime) -> bool:
    return marker is not None and now - marker.advanced_at > STALL_AFTER
