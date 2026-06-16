# app/actions/tests/test_source_profiles.py
from datetime import datetime, timezone
from app.actions.source_profiles import Assignment, SourceProfile, ResolvedSource


def _dt(s):
    return datetime.fromisoformat(s).replace(tzinfo=timezone.utc)


def test_assignment_covers_open_ended():
    a = Assignment(lower=_dt("2026-01-01T00:00:00"), upper=None, subject_name="Tau", subject_type="elephant")
    assert a.covers(_dt("2026-06-01T00:00:00")) is True
    assert a.covers(_dt("2025-12-31T23:59:59")) is False


def test_assignment_covers_closed_range_half_open():
    a = Assignment(lower=_dt("2026-01-01T00:00:00"), upper=_dt("2026-02-01T00:00:00"))
    assert a.covers(_dt("2026-01-15T00:00:00")) is True
    assert a.covers(_dt("2026-02-01T00:00:00")) is False  # upper exclusive
    assert a.covers(_dt("2025-12-31T00:00:00")) is False


from app.actions.source_profiles import resolve_source


def test_resolve_no_profile_falls_back_to_uuid():
    r = resolve_source(None, "abc-123", _dt("2026-06-01T00:00:00"))
    assert r.external_source_id == "er-src-abc-123"
    assert r.source_name is None and r.subject_type is None


def test_resolve_missing_manufacturer_id_falls_back_to_uuid():
    p = SourceProfile(manufacturer_id=None, assignments=[
        Assignment(lower=_dt("2026-01-01T00:00:00"), upper=None, subject_name="Tau", subject_type="elephant"),
    ])
    r = resolve_source(p, "abc-123", _dt("2026-06-01T00:00:00"))
    assert r.external_source_id == "er-src-abc-123"
    assert r.source_name == "Tau" and r.subject_type == "elephant"


def test_resolve_uses_manufacturer_id_and_time_correct_subject():
    p = SourceProfile(manufacturer_id="SERIAL-9", assignments=[
        Assignment(lower=_dt("2026-01-01T00:00:00"), upper=_dt("2026-03-01T00:00:00"), subject_name="Tau", subject_type="elephant"),
        Assignment(lower=_dt("2026-03-01T00:00:00"), upper=None, subject_name="Habi", subject_type="elephant"),
    ])
    early = resolve_source(p, "abc-123", _dt("2026-02-01T00:00:00"))
    late = resolve_source(p, "abc-123", _dt("2026-06-01T00:00:00"))
    assert early.external_source_id == "SERIAL-9" and early.source_name == "Tau"
    assert late.source_name == "Habi"


def test_resolve_no_covering_assignment_omits_name():
    p = SourceProfile(manufacturer_id="SERIAL-9", assignments=[
        Assignment(lower=_dt("2026-03-01T00:00:00"), upper=None, subject_name="Habi"),
    ])
    r = resolve_source(p, "abc-123", _dt("2026-01-01T00:00:00"))
    assert r.external_source_id == "SERIAL-9"
    assert r.source_name is None and r.subject_type is None


def test_resolve_overlapping_picks_latest_starting():
    p = SourceProfile(manufacturer_id="S", assignments=[
        Assignment(lower=_dt("2026-01-01T00:00:00"), upper=None, subject_name="Old"),
        Assignment(lower=_dt("2026-02-01T00:00:00"), upper=None, subject_name="New"),
    ])
    r = resolve_source(p, "abc-123", _dt("2026-06-01T00:00:00"))
    assert r.source_name == "New"


import pytest
from app.actions.source_profiles import SourceProfileResolver


class _FakeER:
    def __init__(self):
        self.assignment_calls = []
        self.source_detail_calls = []
    async def get_source_assignments(self, subject_ids=None, source_ids=None):
        self.assignment_calls.append(tuple(source_ids or []))
        return [{
            "assigned_range": {"lower": "2026-01-01T00:00:00+00:00", "upper": None},
            "source": "src-1", "subject": "subj-1",
        }]
    async def get_source_subjects(self, source_id):
        return [{"id": "subj-1", "name": "Tau", "subject_type": "elephant"}]
    async def get_source_by_manufacturer_id(self, source_id):  # source/{uuid}/
        self.source_detail_calls.append(source_id)
        return {"id": "src-1", "manufacturer_id": "SERIAL-9"}


@pytest.mark.asyncio
async def test_resolver_builds_profile_and_caches():
    er = _FakeER()
    r = SourceProfileResolver(er)
    await r.ensure(["src-1"])
    await r.ensure(["src-1"])  # second call must hit cache, not refetch
    assert er.source_detail_calls == ["src-1"]            # fetched once
    assert er.assignment_calls == [("src-1",)]            # fetched once
    res = r.resolve("src-1", _dt("2026-06-01T00:00:00"))
    assert res.external_source_id == "SERIAL-9"
    assert res.source_name == "Tau" and res.subject_type == "elephant"


@pytest.mark.asyncio
async def test_resolver_fetch_error_falls_back(mocker):
    er = _FakeER()
    mocker.patch.object(er, "get_source_assignments", side_effect=RuntimeError("boom"))
    r = SourceProfileResolver(er)
    await r.ensure(["src-1"])
    res = r.resolve("src-1", _dt("2026-06-01T00:00:00"))
    assert res.external_source_id == "er-src-src-1"       # fallback, never raises
    assert res.source_name is None
