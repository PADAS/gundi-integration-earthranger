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
