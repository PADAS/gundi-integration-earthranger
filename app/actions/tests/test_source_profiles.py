# app/actions/tests/test_source_profiles.py
from datetime import datetime, timezone
from app.actions.source_profiles import Assignment, SourceProfile, ResolvedSource


def _dt(s):
    return datetime.fromisoformat(s).replace(tzinfo=timezone.utc)


def _naive(s):
    """Return a timezone-naive datetime (no tzinfo)."""
    return datetime.fromisoformat(s)


def test_assignment_covers_open_ended():
    a = Assignment(lower=_dt("2026-01-01T00:00:00"), upper=None, subject_name="Tau", subject_type="elephant")
    assert a.covers(_dt("2026-06-01T00:00:00")) is True
    assert a.covers(_dt("2025-12-31T23:59:59")) is False


def test_assignment_covers_closed_range_half_open():
    a = Assignment(lower=_dt("2026-01-01T00:00:00"), upper=_dt("2026-02-01T00:00:00"))
    assert a.covers(_dt("2026-01-15T00:00:00")) is True
    assert a.covers(_dt("2026-02-01T00:00:00")) is False  # upper exclusive
    assert a.covers(_dt("2025-12-31T00:00:00")) is False


# --- timezone-naive vs aware comparison tests ---

def test_assignment_covers_naive_recorded_at_against_aware_bounds():
    """Naive recorded_at must NOT raise TypeError against an aware lower/upper."""
    a = Assignment(
        lower=_dt("2026-01-01T00:00:00"),
        upper=_dt("2026-06-01T00:00:00"),
        subject_name="Tau",
        subject_type="elephant",
    )
    # naive datetime inside the range -> True
    assert a.covers(_naive("2026-03-01T00:00:00")) is True
    # naive datetime before lower -> False
    assert a.covers(_naive("2025-12-31T00:00:00")) is False
    # naive datetime at upper (exclusive) -> False
    assert a.covers(_naive("2026-06-01T00:00:00")) is False


def test_assignment_covers_naive_lower_against_aware_recorded_at():
    """Aware recorded_at must NOT raise TypeError when lower/upper are naive (legacy data)."""
    # lower stored as naive (simulate ER returning offset-less ISO string)
    a = Assignment(
        lower=_naive("2026-01-01T00:00:00"),
        upper=_naive("2026-06-01T00:00:00"),
        subject_name="Tau",
    )
    # aware recorded_at inside the range -> True
    assert a.covers(_dt("2026-03-01T00:00:00")) is True
    # aware recorded_at before lower -> False
    assert a.covers(_dt("2025-12-01T00:00:00")) is False


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
    """A get_source_assignments failure is now per-chunk: ensure() never raises.
    _fetch_ranges skips the failing chunk and returns empty ranges.
    _build_profile still runs; if that also fails we get the UUID fallback.
    Here we patch both so we confirm the pull never raises."""
    er = _FakeER()
    mocker.patch.object(er, "get_source_assignments", side_effect=RuntimeError("boom"))
    mocker.patch.object(er, "get_source_by_manufacturer_id", side_effect=RuntimeError("also boom"))
    r = SourceProfileResolver(er)
    await r.ensure(["src-1"])
    res = r.resolve("src-1", _dt("2026-06-01T00:00:00"))
    assert res.external_source_id == "er-src-src-1"       # fallback, never raises
    assert res.source_name is None


# --- new test: ensure caches sources independently ---

@pytest.mark.asyncio
async def test_ensure_caches_sources_independently(mocker):
    """ensure(["src-1","src-2"]) caches both; a _build_profile failure for one does not
    corrupt or evict the other."""
    er = _FakeER()

    # _fetch_ranges returns data for both sources
    async def fake_assignments(subject_ids=None, source_ids=None):
        er.assignment_calls.append(tuple(source_ids or []))
        return [
            {
                "assigned_range": {"lower": "2026-01-01T00:00:00+00:00", "upper": None},
                "source": "src-1", "subject": "subj-1",
            },
            {
                "assigned_range": {"lower": "2026-02-01T00:00:00+00:00", "upper": None},
                "source": "src-2", "subject": "subj-2",
            },
        ]

    mocker.patch.object(er, "get_source_assignments", side_effect=fake_assignments)

    # _build_profile for src-2 raises; src-1 succeeds
    original_get_source_by_manufacturer_id = er.get_source_by_manufacturer_id

    async def failing_detail(source_id):
        if source_id == "src-2":
            raise RuntimeError("detail fetch failed for src-2")
        return await original_get_source_by_manufacturer_id(source_id)

    mocker.patch.object(er, "get_source_by_manufacturer_id", side_effect=failing_detail)

    r = SourceProfileResolver(er)
    await r.ensure(["src-1", "src-2"])

    # src-1 should have a real profile with SERIAL-9
    res1 = r.resolve("src-1", _dt("2026-06-01T00:00:00"))
    assert res1.external_source_id == "SERIAL-9"
    assert res1.source_name == "Tau"

    # src-2 should have fallen back gracefully (empty SourceProfile)
    res2 = r.resolve("src-2", _dt("2026-06-01T00:00:00"))
    assert res2.external_source_id == "er-src-src-2"
    assert res2.source_name is None


# --- new test: chunking into multiple get_source_assignments calls ---

@pytest.mark.asyncio
async def test_ensure_chunks_large_source_list(mocker):
    """Passing >25 source UUIDs results in TWO get_source_assignments calls."""
    from app.actions.source_profiles import SOURCE_ID_CHUNK_SIZE

    er = _FakeER()
    # Return empty results (no assignments) — we only care about call count/chunking
    async def empty_assignments(subject_ids=None, source_ids=None):
        er.assignment_calls.append(tuple(source_ids or []))
        return []

    mocker.patch.object(er, "get_source_assignments", side_effect=empty_assignments)
    mocker.patch.object(er, "get_source_by_manufacturer_id", return_value={"manufacturer_id": None})
    mocker.patch.object(er, "get_source_subjects", return_value=[])

    # Create SOURCE_ID_CHUNK_SIZE + 1 sources so we get 2 chunks
    source_uuids = [f"src-{i}" for i in range(SOURCE_ID_CHUNK_SIZE + 1)]

    r = SourceProfileResolver(er)
    await r.ensure(source_uuids)

    # Should have made exactly 2 get_source_assignments calls
    assert len(er.assignment_calls) == 2
    # First chunk should have SOURCE_ID_CHUNK_SIZE elements
    assert len(er.assignment_calls[0]) == SOURCE_ID_CHUNK_SIZE
    # Second chunk should have the remainder
    assert len(er.assignment_calls[1]) == 1
    # All sources should be cached
    for uuid in source_uuids:
        assert uuid in r._cache


# --- new test: _fetch_ranges warns on paginated 'next' ---

@pytest.mark.asyncio
async def test_fetch_ranges_warns_on_pagination(mocker):
    """When a chunk response dict carries a truthy 'next', a warning is logged."""
    er = _FakeER()

    async def paginated_response(subject_ids=None, source_ids=None):
        return {
            "results": [
                {
                    "assigned_range": {"lower": "2026-01-01T00:00:00+00:00", "upper": None},
                    "source": "src-1", "subject": "subj-1",
                }
            ],
            "next": "http://example.com/api/v1.0/subjectsources/?page=2",
            "count": 50,
        }

    mocker.patch.object(er, "get_source_assignments", side_effect=paginated_response)
    mock_warning = mocker.patch("app.actions.source_profiles.logger.warning")

    r = SourceProfileResolver(er)
    await r.ensure(["src-1"])

    # At least one warning call should mention pagination / 'next'
    warning_texts = [str(call) for call in mock_warning.call_args_list]
    assert any("next" in t.lower() or "pagina" in t.lower() for t in warning_texts), (
        f"Expected pagination warning, got: {warning_texts}"
    )


# --- new test: per-chunk resilience in _fetch_ranges ---

@pytest.mark.asyncio
async def test_fetch_ranges_per_chunk_resilience(mocker):
    """A failing chunk in _fetch_ranges must not discard results from successful chunks.
    The pull must not raise out of ensure()."""
    from app.actions.source_profiles import SOURCE_ID_CHUNK_SIZE

    er = _FakeER()
    call_count = 0

    async def sometimes_fails(subject_ids=None, source_ids=None):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            # First chunk succeeds
            return [
                {
                    "assigned_range": {"lower": "2026-01-01T00:00:00+00:00", "upper": None},
                    "source": "src-0", "subject": "subj-1",
                }
            ]
        else:
            # Second chunk fails
            raise RuntimeError("network error on chunk 2")

    mocker.patch.object(er, "get_source_assignments", side_effect=sometimes_fails)

    # get_source_by_manufacturer_id only returns MFRID-0 for src-0;
    # all others raise so they fall through to the UUID fallback.
    async def discriminating_detail(source_id):
        if source_id == "src-0":
            return {"manufacturer_id": "MFRID-0"}
        raise RuntimeError(f"detail unavailable for {source_id}")

    mocker.patch.object(er, "get_source_by_manufacturer_id", side_effect=discriminating_detail)
    mocker.patch.object(er, "get_source_subjects", return_value=[{"id": "subj-1", "name": "Tau", "subject_type": "elephant"}])

    source_uuids = [f"src-{i}" for i in range(SOURCE_ID_CHUNK_SIZE + 1)]

    r = SourceProfileResolver(er)
    # Must NOT raise
    await r.ensure(source_uuids)

    # src-0 (in first chunk) should have assignment data from the successful chunk
    res0 = r.resolve("src-0", _dt("2026-06-01T00:00:00"))
    assert res0.external_source_id == "MFRID-0"

    # Sources from the second (failed) chunk: _build_profile raises → UUID fallback
    res_last = r.resolve(f"src-{SOURCE_ID_CHUNK_SIZE}", _dt("2026-06-01T00:00:00"))
    assert res_last.external_source_id == f"er-src-src-{SOURCE_ID_CHUNK_SIZE}"
