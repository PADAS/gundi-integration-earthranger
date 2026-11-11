"""What Pull Events / Pull Observations would send, for the portal's confirmation.

The portal runs the preview actions with the section's unsaved values
(config_overrides) before it saves, and draws the answer as a grouped list:
event types by category, or subjects by subject group. The response shape is
the gundi:share_preview contract; see gundi-portal
docs/superpowers/specs/2026-11-09-er2er-sharing-preview-modal-design.md.

The previews apply the same filters the pull actions apply, and read only what
they already read (event types, event categories, the subject group tree), so
they need no new token permissions.
"""
import logging
from typing import Awaitable, Callable, Dict, Iterable, List, Optional

from erclient import VERSION_1_0, VERSION_2_0

logger = logging.getLogger(__name__)

# Items listed per group; a group's total_count stays exact when it is cut.
ITEM_CAP = 200


def _wanted(values: Optional[Iterable]) -> List[str]:
    """The picked values, blank rows left out, kept exactly as given (the pull
    actions compare them as stored). Empty means "everything". The portal
    drops blank rows on Save too, so a stored list never holds them."""
    return [str(v) for v in (values or []) if v is not None and str(v).strip()]


INCOMPLETE = "EarthRanger sent only part of this list, so some items may be missing below."


class _Reader:
    """Reads ER list responses and remembers when one came back paginated
    with more pages: these endpoints answer with one full list on the sites
    we know, and the pull actions read the same single response, so a further
    page is not fetched; the preview says it may be incomplete instead."""

    def __init__(self):
        self.incomplete = False

    def records(self, response) -> List[dict]:
        """An ER list response as a list of dicts (flat list or paginated envelope)."""
        if isinstance(response, dict):
            if response.get("next"):
                self.incomplete = True
            response = response.get("results", [])
        return [r for r in (response or []) if isinstance(r, dict)]


def _warning(*sentences: Optional[str]) -> Optional[str]:
    return " ".join(s for s in sentences if s) or None


def _category_of(event_type: dict) -> Optional[str]:
    """v1 carries the category as a nested dict, v2 as a bare slug."""
    category = event_type.get("category")
    if isinstance(category, dict):
        return category.get("value")
    return category or None


def _by_label(item: dict) -> str:
    return (item.get("label") or item.get("value") or "").lower()


def _names(values: List[str]) -> str:
    return ", ".join(values)


def _response(site_url: str, mode: str, selected_count: int, total_count: int, groups: List[dict],
              missing: List[str], warning: Optional[str], incomplete: bool = False) -> dict:
    truncated = incomplete
    for group in groups:
        if len(group["items"]) > ITEM_CAP:
            group["items"] = group["items"][:ITEM_CAP]
            truncated = True
    return {
        "source_site": site_url,
        "mode": mode,
        "selected_count": selected_count,
        "total_count": total_count,
        "groups": groups,
        "missing_values": missing,
        "truncated": truncated,
        "warning": warning,
    }


async def preview_events(er_client, site_url: str, event_types: Optional[Iterable],
                         event_categories: Optional[Iterable]) -> dict:
    """Event types grouped by category, each with its icon and default priority.

    pull_events filters by event type AND event category, so a type is listed
    only when it passes both filters that are set, as the pull resolves them
    to UUIDs. With neither set, every event is pulled, of any type. Inactive
    types are listed too (marked inactive) unless picked by name: the pull
    adds no active-type constraint, but resolves picked types from the active
    listings only. v2 types are load-bearing (a failure propagates, so the portal says
    why). A failed v1 lookup leaves a warning, since v1 events are sent too.
    A failed categories lookup propagates only when a picked category can't be
    resolved without it, which is when the pull itself skips.
    """
    reader = _Reader()
    wanted_types = _wanted(event_types)
    wanted_categories = _wanted(event_categories)
    mode = "selected" if (wanted_types or wanted_categories) else "all"

    types = reader.records(await er_client.get_event_types(include_inactive=True, version=VERSION_2_0))
    seen = {t.get("value") for t in types}
    v1_failed = None
    try:
        v1_types = reader.records(await er_client.get_event_types(include_inactive=True, version=VERSION_1_0))
    except Exception as e:  # best-effort, as in _fetch_event_type_maps
        logger.warning("Preview: could not fetch ER v1 event types: %s", type(e).__name__)
        v1_failed = ("EarthRanger didn't send its classic (v1) event types, so their events "
                     "may be sent too without being listed below.")
    else:
        types += [t for t in v1_types if t.get("value") not in seen]
    types = [t for t in types if t.get("value")]

    categories: Dict[str, dict] = {}
    categories_error = None
    try:
        for c in reader.records(await er_client.get_event_categories(include_inactive=True)):
            if c.get("value"):
                categories[c["value"]] = c
    except Exception as e:
        logger.warning("Preview: could not fetch ER event categories: %s", type(e).__name__)
        categories_error = e

    # pull_events resolves a picked category to its UUID from v1's nested
    # category records or from the categories call, drops the ones it can't
    # resolve, and skips when the categories call failed for one of them.
    # The pull reads the active listings, so only active types lend their
    # nested category UUID.
    resolvable_categories = {slug for slug, c in categories.items() if c.get("id")} | {
        t["category"].get("value") for t in types
        if isinstance(t.get("category"), dict) and t["category"].get("id") and t.get("is_active") is not False
    }
    if categories_error and any(c not in resolvable_categories for c in wanted_categories):
        raise categories_error
    # It resolves a picked type from the active listings only, so a picked
    # inactive type is dropped from the filter.
    active_values = {t["value"] for t in types if t.get("is_active") is not False}
    used_types = [v for v in wanted_types if v in active_values]
    used_categories = [c for c in wanted_categories if c in resolvable_categories]

    # A category a type points at always gets a group, named after the type's
    # own category record when the categories call didn't return it.
    for t in types:
        slug = _category_of(t)
        if slug and slug not in categories:
            raw = t.get("category")
            categories[slug] = raw if isinstance(raw, dict) else {"value": slug}
    if any(not _category_of(t) for t in types):
        categories[""] = {"value": "", "display": "Uncategorized"}

    def passes(t: dict) -> bool:
        # A filter with nothing resolvable left makes the pull skip, so
        # nothing passes it.
        return ((not wanted_types or t["value"] in used_types)
                and (not wanted_categories or _category_of(t) in used_categories))

    def active(t: dict) -> bool:
        return t.get("is_active") is not False

    picked = [t for t in types if passes(t)]

    def item(t: dict) -> dict:
        return {
            "value": t["value"],
            "label": t.get("display") or t["value"],
            "icon_url": f"{site_url}/static/sprite-src/{t.get('icon_id') or t['value']}.svg",
            "is_active": active(t),
            "priority": t.get("default_priority") or 0,
            "detail": None,
        }

    groups = []
    for slug, category in categories.items():
        items = sorted((item(t) for t in picked if (_category_of(t) or "") == slug), key=_by_label)
        if not items:
            continue
        groups.append({
            "value": slug,
            "label": category.get("display") or slug,
            "order": category.get("ordernum") or 0,
            "total_count": sum(1 for t in types if (_category_of(t) or "") == slug),
            "items": items,
        })
    # Uncategorized last; the order stays a finite number so the answer is valid JSON.
    groups.sort(key=lambda g: (g["value"] == "", g["order"], (g["label"] or "").lower()))

    by_value = {t["value"]: t for t in types}
    missing = ([v for v in wanted_types if v not in by_value]
               + [v for v in wanted_categories if v not in resolvable_categories])
    inactive = [by_value[v].get("display") or v for v in wanted_types
                if v in by_value and v not in active_values]
    # Picked types outside the picked categories (that resolve): ER applies
    # both filters, so their events are left out.
    outside = [by_value[v].get("display") or v for v in used_types
               if wanted_categories and _category_of(by_value[v]) not in used_categories]
    warning = _warning(
        (f"These event types are inactive, so their events won't be shared: {_names(inactive)}."
         if inactive else None),
        (f"These event types aren't in the chosen categories, so their events won't be shared: "
         f"{_names(outside)}.") if outside else None,
        v1_failed,
        INCOMPLETE if reader.incomplete else None,
    )

    return _response(
        site_url, mode,
        selected_count=len(picked),
        total_count=len(types),
        groups=groups,
        missing=missing,
        warning=warning,
        incomplete=reader.incomplete,
    )


def _walk(groups: List[dict]):
    """Every group in ER's subject group tree, with its descendants' subjects."""
    for group in groups:
        children = [g for g in (group.get("subgroups") or []) if isinstance(g, dict)]
        yield group, children
        yield from _walk(children)


def subjects_under(group: dict) -> List[dict]:
    """A group's subjects and its subgroups', as pull_observations includes them."""
    found = {}
    for g, _ in _walk([group]):
        for s in g.get("subjects") or []:
            if isinstance(s, dict) and s.get("id"):
                found.setdefault(str(s["id"]), s)
    return list(found.values())


async def preview_subjects(
        er_client, site_url: str, subject_group_ids: Optional[Iterable],
        source_assignments: Optional[Callable[[List[str]], Awaitable[List[dict]]]] = None,
) -> dict:
    """Subjects grouped by subject group, each with its icon and last point.

    pull_observations includes a picked group's subgroups, so each picked group
    lists its subgroups' subjects too. With no group picked, every source the
    token reads is pulled, so every group with subjects is listed, and a
    warning says that subjects outside every group are sent too. Counts are
    distinct subjects: a subject in two groups is pulled once.

    With groups picked, the pull sends points by the devices assigned to the
    subjects (``source_assignments``, the pull's own lookup), so a subject
    with no device is left out and counted in a warning.
    """
    reader = _Reader()
    wanted = _wanted(subject_group_ids)
    mode = "selected" if wanted else "all"
    # The same call pull_observations makes to resolve the picked groups.
    tree = reader.records(await er_client.get_subjectgroups(flat=False))
    every = [g for g, _ in _walk(tree)]
    if mode == "all":
        picked = [(g, [s for s in (g.get("subjects") or []) if isinstance(s, dict)]) for g in every]
        picked = [(g, subjects) for g, subjects in picked if subjects]
    else:
        picked = [(g, subjects_under(g)) for g in every if str(g.get("id")) in wanted]

    no_device = 0
    if mode == "selected" and source_assignments is not None:
        subject_ids = sorted({str(s["id"]) for _, subjects in picked for s in subjects if s.get("id")})
        with_device = ({str(a.get("subject")) for a in await source_assignments(subject_ids)}
                       if subject_ids else set())
        no_device = len(set(subject_ids) - with_device)
        picked = [(g, [s for s in subjects if str(s.get("id")) in with_device]) for g, subjects in picked]

    def icon(url: Optional[str]) -> Optional[str]:
        if not url:
            return None
        if url.startswith(("http://", "https://")):
            return url
        return f"{site_url}{'' if url.startswith('/') else '/'}{url}"

    groups = []
    for g, subjects in picked:
        groups.append({
            "value": str(g.get("id")),
            "label": g.get("name"),
            "order": 0,
            "total_count": len(subjects),
            "items": sorted(({
                "value": str(s["id"]),
                "label": s.get("name"),
                "icon_url": icon(s.get("image_url")),
                "is_active": s.get("is_active") is not False,
                "detail": s.get("last_position_date"),
            } for s in subjects if s.get("id")), key=_by_label),
        })
    groups.sort(key=lambda grp: (grp["label"] or "").lower())

    def distinct(lists) -> int:
        return len({str(s["id"]) for subjects in lists for s in subjects if s.get("id")})

    known = {str(g.get("id")) for g in every}
    return _response(
        site_url, mode,
        selected_count=distinct(subjects for _, subjects in picked),
        total_count=distinct(subjects_under(g) for g in tree),
        groups=groups,
        missing=[v for v in wanted if v not in known],
        warning=_warning(
            ("With no subject group picked, subjects that aren't in any subject group are sent too, "
             "and they aren't listed below.") if mode == "all" else
            # The pull picks the devices these subjects use now and sends all
            # their points in the time window, whoever carried them then.
            ("Points are picked by the devices these subjects use now, so points a device recorded "
             "earlier for another subject can be sent too."),
            (f"{no_device} subject{'' if no_device == 1 else 's'} in these groups "
             f"{'has' if no_device == 1 else 'have'} no device, so nothing is sent for "
             f"{'it' if no_device == 1 else 'them'}.") if no_device else None,
            INCOMPLETE if reader.incomplete else None,
        ),
        incomplete=reader.incomplete,
    )
