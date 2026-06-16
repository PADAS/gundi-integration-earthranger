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
