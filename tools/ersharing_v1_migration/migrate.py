#!/usr/bin/env python3
"""Move Gundi v1 EarthRanger-to-EarthRanger shares (er2er events, er2er_subject_sharing
tracks) to Gundi v2 routes on the `earth_ranger` integration type.

Dry run by default: it only reads, and prints what each selected route would create.
`--apply` makes the writes, after a typed confirmation. See README.md next to this file.

Standard library only. Tokens are read from the v1 API at run time and are never printed.
"""
import argparse
import json
import os
import re
import sys
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, List, Optional

HERE = os.path.dirname(os.path.abspath(__file__))
DEFAULT_API = "https://api.gundiservice.org"
DEFAULT_TOKEN_FILE = os.path.expanduser("~/.config/gundi/portal-token-prod")
DEFAULT_PLAN = os.path.join(HERE, "plan.json")
DEFAULT_MAPPING = os.path.join(HERE, "mapping.json")

ER_TYPE = "earth_ranger"
AUTH, SYNC_EVENTS, SYNC_OBSERVATIONS = "auth", "sync_events", "sync_observations"
OBSERVATIONS_STREAM = "obv"
PROVIDER_KEY_FIELD = "provider_key"
V1_DEFAULT_PROVIDER_KEY = "das_provider"   # er2er State.provider_key default
V2_DEFAULT_PROVIDER_KEY = "das_provider"   # app/ersharing/modes.py SIMPLE_PROVIDER_KEY
EVENT_SWITCHES = ["prepend_system_to_categories", "prepend_system_to_event_types",
                  "prepend_system_to_event_titles", "update_schema", "delete_unmatched_events"]
SECRET_KEYS = {"token", "password", "dest_token", "apikey", "api_key"}

# plan.json column positions (written by the planning script build.py).
R_ID, R_SOURCE, R_OWNER, R_EVENTS, R_TRACKS, R_DESTS = 0, 1, 2, 4, 5, 7
R_EVENT_IDS, R_TRACK_IDS, R_WAVE, R_REASONS = 19, 20, 24, 25
D_ROUTE, D_HOST, D_PROVIDER_KEY, D_OUTBOUNDS = 0, 2, 5, 9


class RouteError(Exception):
    """This route can't be migrated as planned. Nothing is written for it."""


# ---------------------------------------------------------------- HTTP

class Api:
    def __init__(self, base: str, token: str):
        self.base = base.rstrip("/")
        self._token = token
        self.secrets = {token}

    def _call(self, method: str, path: str, body=None, raw=False):
        url = path if path.startswith("http") else f"{self.base}{path}"
        data = json.dumps(body).encode() if body is not None else None
        req = urllib.request.Request(url, data=data, method=method, headers={
            "Authorization": f"Bearer {self._token}", "Content-Type": "application/json",
            "Accept": "text/html" if raw else "application/json"})
        try:
            with urllib.request.urlopen(req, timeout=120) as r:
                text = r.read().decode()
        except urllib.error.HTTPError as e:
            detail = self.redact(e.read().decode(errors="replace"))[:600]
            raise ApiError(method, path, e.code, detail) from None
        return text if raw else (json.loads(text) if text else None)

    def get(self, path, raw=False):
        return self._call("GET", path, raw=raw)

    def post(self, path, body):
        return self._call("POST", path, body)

    def patch(self, path, body):
        return self._call("PATCH", path, body)

    def get_all(self, path):
        """Follow cursor pagination; also accepts endpoints that return a plain list."""
        out, page = [], self.get(path)
        while True:
            if isinstance(page, list):
                return out + page
            out += page.get("results", [])
            if not page.get("next"):
                return out
            page = self.get(page["next"])

    def redact(self, text: str) -> str:
        for s in self.secrets:
            if s:
                text = text.replace(s, "***")
        return re.sub(r'("(?:%s)"\s*:\s*)"[^"]*"' % "|".join(SECRET_KEYS), r'\1"***"', text)


class ApiError(Exception):
    def __init__(self, method, path, status, detail):
        super().__init__(f"{method} {path} -> HTTP {status}: {detail}")
        self.status = status


# ---------------------------------------------------------------- helpers

def host_of(url: str) -> str:
    url = (url or "").strip()
    return (urllib.parse.urlparse(url if "://" in url else f"https://{url}").hostname or "").lower()


def site_label(host: str) -> str:
    return host.split(".")[0]


def base_url_variants(host: str) -> List[str]:
    return [f"https://{host}/", f"https://{host}"]


def v1_provider_key(state: dict) -> str:
    """The key v1 puts in sensors/sharing/<key>/status (er2er State: default only when missing)."""
    return state["provider_key"] if "provider_key" in state else V1_DEFAULT_PROVIDER_KEY


def v2_provider_key(mapping_default: Optional[str]) -> str:
    """What the v2 runner sends for a destination with this field-mapping default
    (app/ersharing/destinations.py provider_keys_from + modes.py)."""
    if isinstance(mapping_default, str) and mapping_default.strip():
        return mapping_default.strip()
    return V2_DEFAULT_PROVIDER_KEY


def feature_group_ids(value) -> List[str]:
    return sorted(str(i.get("id") if isinstance(i, dict) else i) for i in (value or []))


def strict_bool(state: dict, key: str, default: bool) -> bool:
    value = state.get(key, default)
    if value is None:
        return default
    if not isinstance(value, bool):
        raise RouteError(f"v1 setting {key} is {value!r}, not on/off (this is a wave 2 case)")
    return value


def auth_token(integration: dict) -> Optional[str]:
    for c in integration.get("configurations") or []:
        if (c.get("action") or {}).get("value") == AUTH:
            data = c.get("data") or {}
            if data.get("authentication_type", "token") == "token":
                return data.get("token")
    return None


# ---------------------------------------------------------------- plan model

@dataclass
class DestSpec:
    host: str
    token: str = field(repr=False)
    events: bool = False
    tracks: bool = False
    provider_key: Optional[str] = None     # exactly what v1 sends, tracks only
    v1_outbound_ids: List[str] = field(default_factory=list)
    # resolved
    v2_id: Optional[str] = None
    v2_name: Optional[str] = None
    v2_action: str = ""                     # "reuse" | "create" | "reuse (created earlier in this run)"
    others_at_site: Optional[int] = None    # for "create": EarthRanger integrations already at this site


@dataclass
class RouteSpec:
    route_id: str
    wave: object
    source_host: str
    owner_id: str
    owner_name: str
    source_token: str = field(repr=False)
    event_ids: List[str]
    track_ids: List[str]
    dests: Dict[str, DestSpec]
    sync_events: Optional[dict]
    sync_observations: Optional[dict]
    name: str = ""
    # resolved
    source_v2_id: Optional[str] = None
    source_action: str = ""                 # "create" | "exists" | "resume"
    v2_route_id: Optional[str] = None

    def field_mappings(self, source_id: str) -> dict:
        obv = {d.v2_id: {"destination_field": PROVIDER_KEY_FIELD, "default": d.provider_key}
               for d in self.dests.values() if d.tracks}
        return {source_id: {OBSERVATIONS_STREAM: obv}} if obv else {}


# ---------------------------------------------------------------- reading v1

class V1:
    def __init__(self, api: Api):
        self.api = api
        configs = api.get_all("/api/v1.0/integrations/inbound/configurations?enabled=true")
        self.inbound = {c["id"]: c for c in configs}
        # Enabled outbound configs only (the v1 runner sends to these and no others). Read
        # without ?inbound_id=: with it, this endpoint creates a Device.
        outbounds = api.get_all("/api/v1.0/integrations/outbound/configurations")
        self.outbound = {o["id"]: o for o in outbounds}
        api.secrets.update(o["token"] for o in outbounds if o.get("token"))
        self._dg: Dict[str, List[str]] = {}

    def device_group_outbounds(self, devicegroup_id: str) -> List[str]:
        if devicegroup_id not in self._dg:
            html = self.api.get(f"/integrations/devicegroups/{devicegroup_id}/destinations", raw=True)
            self._dg[devicegroup_id] = sorted(set(re.findall(
                r"/integrations/outboundconfigurations/([0-9a-f-]{36})/edit", html)))
        return self._dg[devicegroup_id]


def build_route(plan_route: list, plan_dests: List[list], v1: V1, api: Api) -> RouteSpec:
    rid = plan_route[R_ID]
    event_ids = [i for i in plan_route[R_EVENT_IDS].split(", ") if i]
    track_ids = [i for i in plan_route[R_TRACK_IDS].split(", ") if i]
    missing = [i for i in event_ids + track_ids if i not in v1.inbound]
    if missing:
        raise RouteError(f"v1 config(s) no longer enabled or not found: {', '.join(missing)}")
    configs = [v1.inbound[i] for i in event_ids + track_ids]
    for c in configs:
        api.secrets.update(v for v in (c.get("token"), (c.get("state") or {}).get("dest_token")) if v)

    # One source: same host, owner and token across every v1 config of the route.
    sources = {(host_of(c["endpoint"]), c["owner"], c["token"]) for c in configs}
    if len(sources) != 1:
        raise RouteError("v1 configs of this route no longer share one source site, owner and token")
    source_host, owner_id, source_token = sources.pop()
    if source_host != plan_route[R_SOURCE]:
        raise RouteError(f"source site changed in v1: plan {plan_route[R_SOURCE]}, now {source_host}")
    if not source_token:
        raise RouteError("source has no token in v1")

    dests: Dict[str, DestSpec] = {}

    def add_dest(host: str, token: str, kind: str, provider_key: Optional[str] = None, outbound_id: str = ""):
        if not host:
            raise RouteError(f"a v1 {kind} destination has no site URL")
        if host == source_host:
            raise RouteError(f"{host} is the source site itself")
        if not token:
            raise RouteError(f"v1 {kind} destination {host} has no token")
        d = dests.setdefault(host, DestSpec(host=host, token=token))
        if d.token != token:
            raise RouteError(f"{host} gets events and tracks (or two tracks shares) with different tokens in v1; "
                             "one v2 destination can't hold both")
        if kind == "events":
            d.events = True
        else:
            if d.tracks and d.provider_key != provider_key:
                raise RouteError(f"{host} gets two different provider keys in v1")
            d.tracks, d.provider_key = True, provider_key
            if outbound_id:
                d.v1_outbound_ids.append(outbound_id)

    # Events: destination and token are in the v1 state.
    sync_events = None
    for i in event_ids:
        st = v1.inbound[i].get("state") or {}
        add_dest(host_of(st.get("dest_endpoint")), st.get("dest_token"), "events")
        settings = {
            "run_on_schedule": False,
            "event_types": [],
            "area_feature_groups": feature_group_ids(st.get("within_featuregroups")),
            "compatibility": {
                **{k: strict_bool(st, k, True) for k in EVENT_SWITCHES},
                "source_system_value": st.get("source_system_value") or None,
                "source_system_name": st.get("source_system_name") or None,
            },
        }
        if st.get("last_successful_run"):
            settings["start_from"] = st["last_successful_run"]
        cmp = {k: v for k, v in settings.items() if k != "start_from"}
        if sync_events is not None and {k: v for k, v in sync_events.items() if k != "start_from"} != cmp:
            raise RouteError("merged v1 events configs no longer have the same settings")
        if sync_events is not None and sync_events.get("start_from") and settings.get("start_from"):
            settings["start_from"] = min(sync_events["start_from"], settings["start_from"])
        sync_events = settings

    # Tracks: destinations are the device group's outbound configs, read by id.
    sync_observations = None
    planned_outbounds = {r[D_HOST]: set(filter(None, r[D_OUTBOUNDS].split(", "))) for r in plan_dests}
    for i in track_ids:
        c = v1.inbound[i]
        st = c.get("state") or {}
        if "dest_user" in st:
            raise RouteError("v1 state has dest_user, which v2 has no field for (wave 2 case)")
        key = v1_provider_key(st)
        if not isinstance(key, str) or not key:
            raise RouteError(f"v1 provider key {key!r} is empty or not text; v1 would post to a broken URL")
        live = v1.device_group_outbounds(c["default_devicegroup"])
        if not live:
            raise RouteError(f"v1 tracks config {i} has no destinations in its device group")
        for oid in live:
            o = v1.outbound.get(oid)
            if o is None:
                raise RouteError(f"v1 outbound {oid} in the device group is disabled, so v1 doesn't send there")
            if o.get("type_slug") != "earth_ranger":
                raise RouteError(f"v1 outbound {oid} is not an EarthRanger destination ({o.get('type_slug')})")
            h = host_of(o.get("endpoint"))
            if oid not in planned_outbounds.get(h, set()):
                raise RouteError(f"v1 device group now sends to outbound {oid} ({h}), which isn't in the plan")
            add_dest(h, o.get("token"), "tracks", key, oid)
        settings = {
            "run_on_schedule": False,
            "subject_groups": [],
            "destination_subject_group": (st.get("subject_group") or "").strip() or None,
            "area_feature_groups": feature_group_ids(st.get("feature_groups")),
            "compatibility": {
                "subject_additional": strict_bool(st, "subject_additional", True),
                "src_user": st.get("src_user") or None,
            },
        }
        if sync_observations is not None and sync_observations != settings:
            raise RouteError("merged v1 tracks configs no longer have the same settings")
        sync_observations = settings

    # The live v1 destinations must be exactly the plan's.
    planned = {h for h in plan_route[R_DESTS].split(", ") if h}
    if set(dests) != planned:
        raise RouteError(f"destinations changed in v1: plan {sorted(planned)}, now {sorted(dests)}")
    # Both v2 sync actions send to every destination of the route, so each destination
    # must get every kind of data this route moves.
    for d in dests.values():
        missing = [k for k, ids, on in (("events", event_ids, d.events), ("tracks", track_ids, d.tracks))
                   if ids and not on]
        if missing:
            raise RouteError(f"v1 no longer sends {' and '.join(missing)} to {d.host}, "
                             "but v2 would send all of this route's data there")
    for r in plan_dests:
        d = dests[r[D_HOST]]
        if d.tracks and (r[D_PROVIDER_KEY] or V1_DEFAULT_PROVIDER_KEY) != d.provider_key:
            raise RouteError(f"provider key for {d.host} changed in v1: plan {r[D_PROVIDER_KEY]!r}, now {d.provider_key!r}")

    # v2 must send exactly the key v1 sends.
    for d in dests.values():
        if d.tracks and v2_provider_key(d.provider_key) != d.provider_key:
            raise RouteError(f"v2 would send provider key {v2_provider_key(d.provider_key)!r} to {d.host}, "
                             f"v1 sends {d.provider_key!r}")

    owner_name = plan_route[R_OWNER]
    return RouteSpec(route_id=rid, wave=plan_route[R_WAVE], source_host=source_host, owner_id=owner_id,
                     owner_name=owner_name, source_token=source_token, event_ids=event_ids, track_ids=track_ids,
                     dests=dests, sync_events=sync_events, sync_observations=sync_observations)


def _base_name(plan_route: list) -> str:
    hosts = sorted(h for h in plan_route[R_DESTS].split(", ") if h)
    to = ", ".join(site_label(h) for h in hosts[:3]) + (f" and {len(hosts) - 3} more" if len(hosts) > 3 else "")
    events, tracks = plan_route[R_EVENTS] == "yes", plan_route[R_TRACKS] == "yes"
    what = "events and tracks" if events and tracks else ("events" if events else "tracks")
    return f"ER Sharing: {site_label(plan_route[R_SOURCE])} {what} to {to}"


def plan_names(plan: dict) -> Dict[str, str]:
    """v2 name per plan route, from the whole plan, so a route gets the same name whichever
    routes are picked in a run. Names shared by several routes all get the route id added."""
    base = {r[R_ID]: _base_name(r) for r in plan["routes"]}
    counts: Dict[str, int] = {}
    for n in base.values():
        counts[n] = counts.get(n, 0) + 1
    return {rid: (f"{n} ({rid})" if counts[n] > 1 else n) for rid, n in base.items()}


# ---------------------------------------------------------------- reading v2

class V2:
    def __init__(self, api: Api):
        self.api = api
        er = self._er_type()
        self.type_id = er["id"]
        self.actions = {a["value"]: a["id"] for a in er.get("actions", [])}
        self._by_host_owner: Dict[tuple, List[dict]] = {}
        self._site_counts: Dict[str, int] = {}
        self.planned_dests: Dict[tuple, DestSpec] = {}   # (host, owner, token) -> first DestSpec

    def _er_type(self) -> dict:
        for t in self.api.get_all("/v2/integrations/types/"):
            if t["value"] == ER_TYPE:
                return t
        raise SystemExit(f"Integration type {ER_TYPE!r} not found in v2")

    def missing_actions(self) -> List[str]:
        return [a for a in (AUTH, SYNC_EVENTS, SYNC_OBSERVATIONS) if a not in self.actions]

    def er_integrations(self, host: str, owner_id: str) -> List[dict]:
        """Full details of the owner's EarthRanger integrations at this site."""
        key = (host, owner_id)
        if key not in self._by_host_owner:
            q = urllib.parse.urlencode({"type": self.type_id, "owner": owner_id,
                                        "base_url__in": ",".join(base_url_variants(host)), "page_size": 100})
            found = []
            for item in self.api.get_all(f"/v2/integrations/?{q}"):
                full = self.api.get(f"/v2/integrations/{item['id']}/")
                t = auth_token(full)
                if t:
                    self.api.secrets.add(t)
                found.append(full)
            self._by_host_owner[key] = sorted(found, key=lambda i: (not i.get("enabled"), i["id"]))
        return self._by_host_owner[key]

    def count_at_site(self, host: str) -> int:
        """EarthRanger integrations at this site under any owner (shown, never reused)."""
        if host not in self._site_counts:
            q = urllib.parse.urlencode({"type": self.type_id, "base_url__in": ",".join(base_url_variants(host)),
                                        "page_size": 100})
            self._site_counts[host] = len(self.api.get_all(f"/v2/integrations/?{q}"))
        return self._site_counts[host]

    def name_taken(self, name: str) -> bool:
        q = urllib.parse.urlencode({"type": self.type_id, "search": name, "page_size": 100})
        return any(i.get("name") == name for i in self.api.get_all(f"/v2/integrations/?{q}"))

    def route(self, route_id: str) -> dict:
        return self.api.get(f"/v2/routes/{route_id}/")


def resolve(spec: RouteSpec, v2: V2, mapping: dict, name: str) -> None:
    """Find what already exists in v2. Fills ids and the create/reuse decision; never writes."""
    spec.name = name

    # Source: only an integration this migration made (same name, site, owner) counts as this
    # route. Another EarthRanger connection of the owner at the site is never reused: its route
    # may already have destinations that would start getting this share's data.
    existing = [i for i in v2.er_integrations(spec.source_host, spec.owner_id) if i.get("name") == spec.name]
    done = mapping.get("routes", {}).get(spec.route_id)
    if done and not existing:
        raise RouteError(f"mapping file says this route was created as {done.get('source_integration_id')}, "
                         "but no such integration is in v2 now")
    if existing:
        src = existing[0]
        if done and done.get("source_integration_id") != src["id"]:
            raise RouteError(f"mapping file says this route is {done.get('source_integration_id')}, "
                             f"but v2 has {src['id']} under its name")
        claimed = [rid for rid, e in mapping.get("routes", {}).items()
                   if e.get("source_integration_id") == src["id"] and rid != spec.route_id]
        if claimed:
            raise RouteError(f"v2 integration {src['id']} is recorded for route {claimed[0]} in the mapping file")
        if auth_token(src) != spec.source_token:
            raise RouteError(f"v2 integration {src['id']} ({spec.name}) exists with a different source token")
        check_source_settings(spec, src)
        spec.source_v2_id = src["id"]
        spec.v2_route_id = (src.get("default_route") or {}).get("id")
        spec.source_action = "exists"
    else:
        spec.source_action = "create"
        if v2.name_taken(spec.name):
            raise RouteError(f"another integration is already called {spec.name!r}")

    # Destinations: the owner's EarthRanger integration at that site with the same token.
    for d in spec.dests.values():
        same = [i for i in v2.er_integrations(d.host, spec.owner_id) if auth_token(i) == d.token]
        if same:
            d.v2_id, d.v2_name, d.v2_action = same[0]["id"], same[0].get("name"), "reuse"
            if same[0].get("enabled") is False:
                d.v2_action = "reuse (disabled in v2: the runner skips it until it's turned on)"
            continue
        key = (d.host, spec.owner_id, d.token)
        if key in v2.planned_dests:
            first = v2.planned_dests[key]
            d.v2_id, d.v2_name, d.v2_action = first.v2_id, first.v2_name, "reuse (created earlier in this run)"
            continue
        d.v2_name, d.v2_action = f"{site_label(d.host)} - ER Sharing destination ({spec.owner_name})", "create"
        d.others_at_site = v2.count_at_site(d.host)
        v2.planned_dests[key] = d

    if spec.source_action == "exists":
        check_existing_route(spec, v2)
        if spec.source_action == "resume":
            check_source_paused(existing[0])


def saved_sync_settings(integration: dict) -> Dict[str, dict]:
    """The sync action settings saved on a v2 integration, by action name."""
    return {(c.get("action") or {}).get("value"): c.get("data") or {}
            for c in integration.get("configurations") or []
            if (c.get("action") or {}).get("value") in (SYNC_EVENTS, SYNC_OBSERVATIONS)}


# Changed by people at cutover (run_on_schedule) or by the runner as it goes (start_from).
_NOT_COMPARED = ("run_on_schedule", "start_from")


def check_source_settings(spec: RouteSpec, src: dict) -> None:
    """A source made by an earlier run must hold the sync settings v1 has now."""
    saved = saved_sync_settings(src)
    for action, want in ((SYNC_EVENTS, spec.sync_events), (SYNC_OBSERVATIONS, spec.sync_observations)):
        have = saved.get(action)
        if want is None and have is not None:
            raise RouteError(f"v2 integration {src['id']} has {action} settings, but v1 no longer shares that data")
        if want is not None and have is None:
            raise RouteError(f"v2 integration {src['id']} has no {action} settings")
        if want is not None and ({k: v for k, v in have.items() if k not in _NOT_COMPARED}
                                 != {k: v for k, v in want.items() if k not in _NOT_COMPARED}):
            raise RouteError(f"v2 integration {src['id']} has {action} settings that differ from v1 now")


def check_source_paused(src: dict) -> None:
    """Before finishing a half-done route, its source must still be off, so adding
    destinations can't start sharing while v1 still runs."""
    if src.get("enabled") is not False:
        raise RouteError(f"v2 integration {src['id']} is turned on; turn it off before finishing this route")
    running = [a for a, data in saved_sync_settings(src).items() if data.get("run_on_schedule") is not False]
    if running:
        raise RouteError(f"v2 integration {src['id']} has {', '.join(sorted(running))} running on a schedule; "
                         "turn that off before finishing this route")


def check_existing_route(spec: RouteSpec, v2: V2) -> None:
    """An integration with this route's name exists. Done if its route matches; resumable if
    it has no destinations yet (an earlier run stopped half-way); otherwise refuse."""
    if not spec.v2_route_id:
        spec.source_action = "resume"
        return
    route = v2.route(spec.v2_route_id)
    have = {str(d["id"] if isinstance(d, dict) else d) for d in route.get("destinations") or []}
    want = {d.v2_id for d in spec.dests.values()}
    if not have:
        spec.source_action = "resume"
        return
    config = route.get("configuration")
    fm = ((config if isinstance(config, dict) else {}).get("data") or {}).get("field_mappings") or {}
    if have == want and (not spec.sync_observations or fm == spec.field_mappings(spec.source_v2_id)):
        spec.source_action = "exists"
        return
    raise RouteError(f"v2 route {spec.v2_route_id} exists but its destinations or provider keys differ from the plan")


# ---------------------------------------------------------------- output

def describe(spec: RouteSpec, v2: V2) -> str:
    def ident(x):
        return x or "<new>"

    lines = [f"=== {spec.route_id}  wave {spec.wave}  {spec.source_host}  owner {spec.owner_name} ({spec.owner_id})"]
    lines.append(f"  v1 events configs: {', '.join(spec.event_ids) or '-'}")
    lines.append(f"  v1 tracks configs: {', '.join(spec.track_ids) or '-'}")
    status = {"create": "CREATE", "exists": "EXISTS, skip", "resume": "RESUME (exists, route not finished)"}
    lines.append(f"  Source integration [{status[spec.source_action]}] {ident(spec.source_v2_id)}")
    lines.append(f"    name: {spec.name}")
    lines.append(f"    type: {ER_TYPE} ({v2.type_id}), base_url: https://{spec.source_host}/, enabled: false")
    lines.append("    auth: token from v1 source config (not shown)")
    for action, cfg in ((SYNC_EVENTS, spec.sync_events), (SYNC_OBSERVATIONS, spec.sync_observations)):
        if cfg is not None:
            lines.append(f"    {action} ({v2.actions.get(action, 'NOT REGISTERED IN v2 YET')}):")
            lines += ["      " + ln for ln in json.dumps(cfg, indent=2, sort_keys=True).splitlines()]
    lines.append(f"  Route (default route of the source): {ident(spec.v2_route_id)}")
    for d in sorted(spec.dests.values(), key=lambda d: d.host):
        kinds = " + ".join(k for k, on in (("events", d.events), ("tracks", d.tracks)) if on)
        lines.append(f"    destination {d.host} [{d.v2_action.upper()}] {ident(d.v2_id)}  ({kinds})")
        lines.append(f"      name: {d.v2_name}; token from v1 (not shown)")
        if d.others_at_site:
            lines.append(f"      {d.others_at_site} other EarthRanger integration(s) at this site in v2, "
                         "none with this owner and token")
        if d.tracks:
            lines.append(f"      v1 outbound(s): {', '.join(d.v1_outbound_ids)}")
            lines.append(f"      provider_key: v1 sends {d.provider_key!r}, v2 will send {v2_provider_key(d.provider_key)!r}")
    if spec.sync_observations:
        sid = spec.source_v2_id or "<source id>"
        fm = {sid: {OBSERVATIONS_STREAM: {(d.v2_id or f"<{d.host}>"): {"destination_field": PROVIDER_KEY_FIELD,
                                                                        "default": d.provider_key}
                                          for d in spec.dests.values() if d.tracks}}}
        lines.append("  Route field_mappings:")
        lines += ["    " + ln for ln in json.dumps(fm, indent=2).splitlines()]
    return "\n".join(lines)


# ---------------------------------------------------------------- writing

def apply_route(spec: RouteSpec, v2: V2, mapping: dict, mapping_path: str) -> None:
    api = v2.api
    for d in spec.dests.values():
        # A destination planned as new may have been made by an earlier route of this run.
        first = v2.planned_dests.get((d.host, spec.owner_id, d.token))
        if not d.v2_id and first is not None and first.v2_id:
            d.v2_id = first.v2_id
        if d.v2_id:
            continue
        created = api.post("/v2/integrations/", {
            "name": d.v2_name, "base_url": f"https://{d.host}/", "enabled": True, "type": v2.type_id,
            "owner": spec.owner_id, "create_default_route": False, "create_configurations": False,
            "configurations": [{"action": v2.actions[AUTH], "data": {"authentication_type": "token", "token": d.token}}],
        })
        d.v2_id = created["id"]
        print(f"  created destination {d.host}: {d.v2_id}")
        # Later routes in this run reuse it.
        if first is not None:
            first.v2_id = d.v2_id
        v2._by_host_owner.pop((d.host, spec.owner_id), None)

    if spec.source_action == "create":
        configurations = [{"action": v2.actions[AUTH], "data": {"authentication_type": "token", "token": spec.source_token}}]
        if spec.sync_events is not None:
            configurations.append({"action": v2.actions[SYNC_EVENTS], "data": spec.sync_events})
        if spec.sync_observations is not None:
            configurations.append({"action": v2.actions[SYNC_OBSERVATIONS], "data": spec.sync_observations})
        try:
            created = api.post("/v2/integrations/", {
                "name": spec.name, "base_url": f"https://{spec.source_host}/", "enabled": False,
                "type": v2.type_id, "owner": spec.owner_id, "create_default_route": True,
                "create_configurations": False, "configurations": configurations,
            })
        except ApiError as e:
            if e.status >= 500:
                raise RouteError(f"{e}. The integration may still have been made: re-run (dry run) to check "
                                 "before trying again") from None
            raise
        spec.source_v2_id = created["id"]
        spec.v2_route_id = (created.get("default_route") or {}).get("id")
        print(f"  created source integration: {spec.source_v2_id}")
    else:
        # Read the source again right before writing: it may have changed since the dry run.
        src = api.get(f"/v2/integrations/{spec.source_v2_id}/")
        check_source_paused(src)
        check_source_settings(spec, src)
    if not spec.v2_route_id:
        spec.v2_route_id = (api.get(f"/v2/integrations/{spec.source_v2_id}/").get("default_route") or {}).get("id")
    if not spec.v2_route_id:
        raise RouteError(f"integration {spec.source_v2_id} has no default route; add one, then re-run")

    patch = {"destinations": sorted(d.v2_id for d in spec.dests.values())}
    fm = spec.field_mappings(spec.source_v2_id)
    if fm:
        patch["configuration"] = {"name": f"{spec.name} - Route - Config", "data": {"field_mappings": fm}}
    api.patch(f"/v2/routes/{spec.v2_route_id}/", patch)

    # Read back and check the keys v2 will send.
    route = v2.route(spec.v2_route_id)
    have = {str(d["id"] if isinstance(d, dict) else d) for d in route.get("destinations") or []}
    if have != set(patch["destinations"]):
        raise RouteError(f"route {spec.v2_route_id} has destinations {sorted(have)} after the update")
    stored = ((route.get("configuration") or {}).get("data") or {}).get("field_mappings") or {}
    obv = (stored.get(spec.source_v2_id) or {}).get(OBSERVATIONS_STREAM) or {}
    for d in spec.dests.values():
        if d.tracks and v2_provider_key((obv.get(d.v2_id) or {}).get("default")) != d.provider_key:
            raise RouteError(f"route {spec.v2_route_id} would send a different provider key to {d.host}")
    print(f"  route {spec.v2_route_id}: {len(have)} destination(s), provider keys checked")
    record(spec, mapping, mapping_path)


def record(spec: RouteSpec, mapping: dict, path: str) -> None:
    entry = {
        "source_host": spec.source_host, "owner_id": spec.owner_id,
        "source_integration_id": spec.source_v2_id, "connection_id": spec.source_v2_id,
        "route_id": spec.v2_route_id, "name": spec.name,
        "destinations": {d.host: d.v2_id for d in spec.dests.values()},
        "v1_events_config_ids": spec.event_ids, "v1_tracks_config_ids": spec.track_ids,
        "recorded_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    }
    mapping.setdefault("routes", {})[spec.route_id] = entry
    for i in spec.event_ids + spec.track_ids:
        mapping.setdefault("v1_configs", {})[i] = {
            "plan_route": spec.route_id, "integration_id": spec.source_v2_id, "connection_id": spec.source_v2_id,
            "route_id": spec.v2_route_id,
            "destination_ids": sorted(d.v2_id for d in spec.dests.values()
                                      if (d.events and i in spec.event_ids) or (d.tracks and i in spec.track_ids)),
        }
    tmp = path + ".tmp"
    with open(tmp, "w") as f:
        json.dump(mapping, f, indent=2, sort_keys=True)
    os.replace(tmp, path)


# ---------------------------------------------------------------- main

def select_routes(plan: dict, args) -> List[list]:
    routes = {r[R_ID]: r for r in plan["routes"]}
    by_v1 = {}
    for r in plan["routes"]:
        for i in filter(None, (r[R_EVENT_IDS] + ", " + r[R_TRACK_IDS]).split(", ")):
            by_v1[i] = r[R_ID]
    wanted: List[str] = []
    tokens = list(args.route or []) + list(args.v1_id or [])
    if args.file:
        with open(args.file) as f:
            tokens += [t for line in f for t in re.split(r"[\s,]+", line.split("#")[0]) if t]
    for t in tokens:
        rid = t if t in routes else by_v1.get(t)
        if not rid:
            raise SystemExit(f"{t} is not a plan route id or a v1 config id in the plan")
        if rid not in wanted:
            wanted.append(rid)
    if args.wave is not None:
        wanted += [r[R_ID] for r in plan["routes"] if r[R_WAVE] == args.wave and r[R_ID] not in wanted]
    if not wanted:
        raise SystemExit("Choose routes: --route, --v1-id, --file or --wave")
    not_ready = [rid for rid in wanted if routes[rid][R_WAVE] != 1]
    if not_ready and not args.include_routes_not_in_wave_1:
        details = "\n".join(f"  {rid}: wave {routes[rid][R_WAVE]} ({routes[rid][R_REASONS]})" for rid in not_ready)
        raise SystemExit(f"These routes aren't in wave 1, so they're refused:\n{details}\n"
                         "Pass --include-routes-not-in-wave-1 to take them anyway.")
    return [routes[rid] for rid in wanted]


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sel = p.add_argument_group("which routes (combine freely)")
    sel.add_argument("--route", nargs="+", metavar="R001", help="plan route ids")
    sel.add_argument("--v1-id", nargs="+", metavar="UUID", help="v1 inbound config ids (selects their route)")
    sel.add_argument("--file", help="file with route ids or v1 config ids, separated by spaces, commas or lines")
    sel.add_argument("--wave", type=int, help="every route of this wave")
    sel.add_argument("--include-routes-not-in-wave-1", action="store_true",
                     help="allow routes the plan puts in wave 2 or 3")
    p.add_argument("--apply", action="store_true", help="make the writes (asks for confirmation first)")
    p.add_argument("--plan", default=DEFAULT_PLAN)
    p.add_argument("--mapping", default=DEFAULT_MAPPING, help="v1 -> v2 id mapping file, written by --apply")
    p.add_argument("--api", default=DEFAULT_API)
    p.add_argument("--token-file", default=DEFAULT_TOKEN_FILE, help="portal bearer token")
    args = p.parse_args(argv)

    with open(args.plan) as f:
        plan = json.load(f)
    plan_routes = select_routes(plan, args)
    dests_by_route: Dict[str, List[list]] = {}
    for r in plan["dests"]:
        dests_by_route.setdefault(r[D_ROUTE], []).append(r)
    mapping = {}
    if os.path.exists(args.mapping):
        with open(args.mapping) as f:
            mapping = json.load(f)

    with open(args.token_file) as f:
        api = Api(args.api, f.read().strip())
    mode = "APPLY" if args.apply else "DRY RUN (reads only)"
    print(f"{mode} against {api.base}, {len(plan_routes)} route(s), plan {args.plan}")
    v1, v2 = V1(api), V2(api)
    missing = v2.missing_actions()
    if missing:
        print(f"NOTE: the v2 {ER_TYPE} type has no {', '.join(missing)} action yet; --apply will refuse until it does.")

    names = plan_names(plan)
    ready: List[RouteSpec] = []
    refused: List[tuple] = []
    for r in plan_routes:
        try:
            spec = build_route(r, dests_by_route.get(r[R_ID], []), v1, api)
            resolve(spec, v2, mapping, names[r[R_ID]])
            print(describe(spec, v2))
            ready.append(spec)
        except (RouteError, ApiError) as e:
            refused.append((r[R_ID], api.redact(str(e))))
            print(f"=== {r[R_ID]}  REFUSED: {api.redact(str(e))}")
        print()

    todo = [s for s in ready if s.source_action != "exists"]
    print(f"Summary: {len(ready)} ready ({len(todo)} to create or finish, {len(ready) - len(todo)} already done), "
          f"{len(refused)} refused")
    for rid, why in refused:
        print(f"  {rid}: {why}")
    if not args.apply:
        print("Dry run: nothing was written.")
        return 0 if not refused else 2
    if missing:
        print(f"Not applying: {', '.join(missing)} must exist on the v2 {ER_TYPE} type first.")
        return 1
    # Routes already complete in v2 go into the mapping file too (e.g. after it was lost).
    for spec in ready:
        if spec.source_action == "exists" and spec.route_id not in mapping.get("routes", {}):
            record(spec, mapping, args.mapping)
    if not todo:
        print("Nothing to write.")
        return 0 if not refused else 2

    phrase = f"apply {len(todo)} to {urllib.parse.urlparse(api.base).hostname}"
    print(f"\nThis will write to {api.base}: {len(todo)} route(s), each created disabled with "
          f"run_on_schedule off.\nType '{phrase}' to go ahead:")
    if input("> ").strip() != phrase:
        print("Not confirmed. Nothing was written.")
        return 1
    failures = 0
    for spec in todo:
        print(f"--- {spec.route_id}")
        try:
            apply_route(spec, v2, mapping, args.mapping)
        except (RouteError, ApiError) as e:
            failures += 1
            print(f"  FAILED: {api.redact(str(e))}")
    print(f"Done: {len(todo) - failures} written, {failures} failed, {len(refused)} refused. "
          f"Mapping file: {args.mapping}")
    if failures:
        return 1
    return 0 if not refused else 2


if __name__ == "__main__":
    sys.exit(main())
