"""Tests for the v1 -> v2 ER Sharing migration script. No network: v1 and v2 are faked."""
import argparse
import importlib.util
import os

import pytest

_spec = importlib.util.spec_from_file_location("migrate", os.path.join(os.path.dirname(__file__), "migrate.py"))
m = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(m)

SRC, DEST_A, DEST_B = "srca.pamdas.org", "desta.pamdas.org", "destb.pamdas.org"
OWNER = "00000000-0000-0000-0000-0000000000aa"


def route_row(rid, wave=1, events="no", tracks="yes", dests=DEST_A, event_ids="", track_ids="t1", source=SRC):
    row = [""] * 26
    row[m.R_ID], row[m.R_SOURCE], row[m.R_OWNER] = rid, source, "Owner"
    row[m.R_EVENTS], row[m.R_TRACKS], row[m.R_DESTS] = events, tracks, dests
    row[m.R_EVENT_IDS], row[m.R_TRACK_IDS], row[m.R_WAVE], row[m.R_REASONS] = event_ids, track_ids, wave, "why"
    return row


def dest_row(rid, host, key="", outbounds=""):
    row = [""] * 13
    row[m.D_ROUTE], row[m.D_HOST], row[m.D_PROVIDER_KEY], row[m.D_OUTBOUNDS] = rid, host, key, outbounds
    return row


def args(**kw):
    base = dict(route=None, v1_id=None, file=None, wave=None, include_routes_not_in_wave_1=False)
    base.update(kw)
    return argparse.Namespace(**base)


class FakeApi:
    def __init__(self):
        self.secrets = set()
        self.posts, self.patches = [], []
        self.routes = {}

    def redact(self, text):
        return m.Api.redact(self, text)

    def post(self, path, body):
        self.posts.append((path, body))
        return {"id": f"new-{len(self.posts)}", "default_route": {"id": f"route-{len(self.posts)}"}}

    def patch(self, path, body):
        self.patches.append((path, body))
        self.routes[path.split("/")[3]] = body

    def get(self, path, raw=False):
        route_id = path.split("/")[3]
        body = self.routes[route_id]
        return {"destinations": [{"id": d} for d in body["destinations"]],
                "configuration": body.get("configuration")}


def fake_v1(inbound, outbound, groups):
    v1 = m.V1.__new__(m.V1)
    v1.inbound, v1.outbound, v1._dg = inbound, outbound, groups
    return v1


def tracks_config(cid, key=..., group="dg1", **state):
    st = dict(state)
    if key is not ...:
        st["provider_key"] = key
    return {"id": cid, "endpoint": f"https://{SRC}/api/v1.0", "owner": OWNER, "token": "src-token",
            "default_devicegroup": group, "state": st}


# ---- provider keys

def test_v1_key_defaults_only_when_missing():
    assert m.v1_provider_key({}) == "das_provider"
    assert m.v1_provider_key({"provider_key": "kws"}) == "kws"
    assert m.v1_provider_key({"provider_key": ""}) == ""


def test_v2_key_is_trimmed_and_falls_back():
    assert m.v2_provider_key("kws") == "kws"
    assert m.v2_provider_key(" kws ") == "kws"
    assert m.v2_provider_key("  ") == "das_provider"
    assert m.v2_provider_key(None) == "das_provider"


def test_route_with_a_key_v2_would_change_is_refused():
    v1 = fake_v1({"t1": tracks_config("t1", key="kws ")},
                 {"o1": {"id": "o1", "type_slug": "earth_ranger", "endpoint": f"https://{DEST_A}/", "token": "d"}},
                 {"dg1": ["o1"]})
    with pytest.raises(m.RouteError, match="v2 would send provider key 'kws'"):
        m.build_route(route_row("R1"), [dest_row("R1", DEST_A, "kws ", "o1")], v1, FakeApi())


def test_tracks_route_is_built_from_live_v1():
    v1 = fake_v1({"t1": tracks_config("t1", key="kws", subject_group=" Group ")},
                 {"o1": {"id": "o1", "type_slug": "earth_ranger", "endpoint": f"https://{DEST_A}/api/v1.0", "token": "d"}},
                 {"dg1": ["o1"]})
    spec = m.build_route(route_row("R1"), [dest_row("R1", DEST_A, "kws", "o1")], v1, FakeApi())
    d = spec.dests[DEST_A]
    assert (d.tracks, d.events, d.provider_key, d.token) == (True, False, "kws", "d")
    assert spec.sync_observations["run_on_schedule"] is False
    assert spec.sync_observations["destination_subject_group"] == "Group"


def test_disabled_outbound_in_the_device_group_is_refused():
    v1 = fake_v1({"t1": tracks_config("t1", key="kws")}, {}, {"dg1": ["o1"]})
    with pytest.raises(m.RouteError, match="disabled"):
        m.build_route(route_row("R1"), [dest_row("R1", DEST_A, "kws", "o1")], v1, FakeApi())


def test_switch_stored_as_text_is_refused():
    v1 = fake_v1({"t1": tracks_config("t1", key="kws", subject_additional="true")},
                 {"o1": {"id": "o1", "type_slug": "earth_ranger", "endpoint": f"https://{DEST_A}/", "token": "d"}},
                 {"dg1": ["o1"]})
    with pytest.raises(m.RouteError, match="not on/off"):
        m.build_route(route_row("R1"), [dest_row("R1", DEST_A, "kws", "o1")], v1, FakeApi())


# ---- choosing routes

PLAN = {"routes": [route_row("R001", track_ids="t1"), route_row("R002", wave=3, track_ids="t2")], "dests": []}


def test_routes_by_id_v1_id_and_file(tmp_path):
    f = tmp_path / "routes.txt"
    f.write_text("R001  # first\n")
    assert [r[0] for r in m.select_routes(PLAN, args(route=["R001"]))] == ["R001"]
    assert [r[0] for r in m.select_routes(PLAN, args(v1_id=["t1"]))] == ["R001"]
    assert [r[0] for r in m.select_routes(PLAN, args(file=str(f)))] == ["R001"]
    assert [r[0] for r in m.select_routes(PLAN, args(wave=1))] == ["R001"]


def test_routes_outside_wave_1_need_the_override():
    with pytest.raises(SystemExit, match="aren't in wave 1"):
        m.select_routes(PLAN, args(route=["R002"]))
    assert [r[0] for r in m.select_routes(PLAN, args(route=["R002"], include_routes_not_in_wave_1=True))] == ["R002"]


def test_unknown_id_is_refused():
    with pytest.raises(SystemExit, match="not a plan route id"):
        m.select_routes(PLAN, args(route=["R999"]))


# ---- names

def test_names_come_from_the_whole_plan():
    plan = {"routes": [route_row("R001"), route_row("R002"), route_row("R003", dests=DEST_B)]}
    names = m.plan_names(plan)
    assert names["R001"] == "ER Sharing: srca tracks to desta (R001)"
    assert names["R002"] == "ER Sharing: srca tracks to desta (R002)"
    assert names["R003"] == "ER Sharing: srca tracks to destb"


# ---- apply

class FakeV2:
    type_id = "er-type"
    actions = {"auth": "a-auth", "sync_events": "a-ev", "sync_observations": "a-obv"}

    def __init__(self, api):
        self.api = api
        self.planned_dests = {}
        self._by_host_owner = {}

    def route(self, route_id):
        return self.api.get(f"/v2/routes/{route_id}/")


def planned_route(rid, v2):
    d = m.DestSpec(host=DEST_A, token="d", tracks=True, provider_key="kws")
    key = (DEST_A, OWNER, "d")
    if key in v2.planned_dests:
        d.v2_action = "reuse (created earlier in this run)"
    else:
        d.v2_action, d.v2_name = "create", "desta - ER Sharing destination (Owner)"
        v2.planned_dests[key] = d
    return m.RouteSpec(route_id=rid, wave=1, source_host=SRC, owner_id=OWNER, owner_name="Owner",
                       source_token="src-token", event_ids=[], track_ids=[f"t-{rid}"], dests={DEST_A: d},
                       sync_events=None, sync_observations={"run_on_schedule": False}, name=f"name {rid}",
                       source_action="create")


def test_a_new_destination_shared_by_two_routes_is_created_once(tmp_path):
    api = FakeApi()
    v2 = FakeV2(api)
    first, second = planned_route("R001", v2), planned_route("R002", v2)
    mapping_path = str(tmp_path / "mapping.json")
    mapping = {}
    m.apply_route(first, v2, mapping, mapping_path)
    m.apply_route(second, v2, mapping, mapping_path)
    destination_posts = [b for _, b in api.posts if not b["create_default_route"]]
    assert len(destination_posts) == 1
    assert first.dests[DEST_A].v2_id == second.dests[DEST_A].v2_id


def test_apply_creates_the_source_off_with_the_provider_key_mapping(tmp_path):
    api = FakeApi()
    v2 = FakeV2(api)
    spec = planned_route("R001", v2)
    mapping = {}
    m.apply_route(spec, v2, mapping, str(tmp_path / "mapping.json"))
    source = next(b for _, b in api.posts if b["create_default_route"])
    assert source["enabled"] is False
    obv = next(c for c in source["configurations"] if c["action"] == "a-obv")
    assert obv["data"]["run_on_schedule"] is False
    _, patch = api.patches[0]
    fm = patch["configuration"]["data"]["field_mappings"]
    assert fm == {spec.source_v2_id: {"obv": {spec.dests[DEST_A].v2_id: {
        "destination_field": "provider_key", "default": "kws"}}}}
    assert mapping["v1_configs"]["t-R001"]["route_id"] == spec.v2_route_id


# ---- secrets

def test_tokens_are_redacted():
    api = m.Api("https://example.org", "portal-secret")
    api.secrets.add("dest-secret")
    text = api.redact('{"token": "abc", "x": "dest-secret", "y": "portal-secret"}')
    assert "abc" not in text and "dest-secret" not in text and "portal-secret" not in text


# ---- review fixes

def test_destination_missing_one_kind_of_data_is_refused():
    events = {"id": "e1", "endpoint": f"https://{SRC}/api/v1.0", "owner": OWNER, "token": "src-token",
              "state": {"dest_endpoint": f"https://{DEST_A}/api/v1.0", "dest_token": "d"}}
    v1 = fake_v1({"e1": events, "t1": tracks_config("t1", key="kws")},
                 {"o1": {"id": "o1", "type_slug": "earth_ranger", "endpoint": f"https://{DEST_A}/", "token": "d"},
                  "o2": {"id": "o2", "type_slug": "earth_ranger", "endpoint": f"https://{DEST_B}/", "token": "b"}},
                 {"dg1": ["o1", "o2"]})
    row = route_row("R1", events="yes", dests=f"{DEST_A}, {DEST_B}", event_ids="e1")
    plan_dests = [dest_row("R1", DEST_A, "kws", "o1"), dest_row("R1", DEST_B, "kws", "o2")]
    with pytest.raises(m.RouteError, match=f"no longer sends events to {DEST_B}"):
        m.build_route(row, plan_dests, v1, FakeApi())


def saved_source(enabled=False, **obv):
    data = {"run_on_schedule": False, **obv}
    return {"id": "src-1", "enabled": enabled,
            "configurations": [{"action": {"value": m.SYNC_OBSERVATIONS}, "data": data}]}


def test_source_settings_must_match_v1_apart_from_schedule():
    spec = planned_route("R001", FakeV2(FakeApi()))
    spec.sync_observations = {"run_on_schedule": False, "subject_groups": []}
    m.check_source_settings(spec, saved_source(run_on_schedule=True, subject_groups=[]))
    with pytest.raises(m.RouteError, match="differ from v1"):
        m.check_source_settings(spec, saved_source(subject_groups=["g"]))
    spec.sync_events = {"run_on_schedule": False}
    with pytest.raises(m.RouteError, match="no sync_events settings"):
        m.check_source_settings(spec, saved_source(subject_groups=[]))


def test_half_done_route_is_finished_only_while_its_source_is_off(tmp_path):
    api = FakeApi()
    v2 = FakeV2(api)
    spec = planned_route("R001", v2)
    spec.source_action, spec.source_v2_id, spec.v2_route_id = "resume", "src-1", "route-1"
    api.get = lambda path, raw=False: saved_source(enabled=True)
    with pytest.raises(m.RouteError, match="is turned on"):
        m.apply_route(spec, v2, {}, str(tmp_path / "mapping.json"))
    assert api.patches == []
    api.get = lambda path, raw=False: saved_source(run_on_schedule=True)
    with pytest.raises(m.RouteError, match="running on a schedule"):
        m.apply_route(spec, v2, {}, str(tmp_path / "mapping.json"))
    assert api.patches == []


def test_apply_exits_nonzero_when_a_route_is_refused(tmp_path, monkeypatch):
    plan = tmp_path / "plan.json"
    plan.write_text(m.json.dumps({"routes": [route_row("R001")], "dests": []}))
    token = tmp_path / "token"
    token.write_text("t")

    class Api:
        base = "https://api.example.org"

        def __init__(self, *a):
            pass

        def redact(self, text):
            return text

    class V2:
        def __init__(self, api):
            pass

        def missing_actions(self):
            return []

    def refuse(*a):
        raise m.RouteError("drift")

    monkeypatch.setattr(m, "Api", Api)
    monkeypatch.setattr(m, "V1", lambda api: None)
    monkeypatch.setattr(m, "V2", V2)
    monkeypatch.setattr(m, "build_route", refuse)
    code = m.main(["--route", "R001", "--apply", "--plan", str(plan), "--token-file", str(token),
                   "--mapping", str(tmp_path / "mapping.json")])
    assert code == 2
