# app/actions/source_profiles.py
import logging
from datetime import datetime, timezone
from typing import Iterable, List, Optional

from pydantic import BaseModel

logger = logging.getLogger(__name__)

# ER's /subjectsources accepts a comma-joined list of source UUIDs; keep chunks
# small so the query string can't 414 and the (single-page) response isn't truncated.
SOURCE_ID_CHUNK_SIZE = 25


def _ensure_utc(dt: Optional[datetime]) -> Optional[datetime]:
    """Return dt normalised to UTC-aware, or None if dt is None.

    If dt has no tzinfo, attach UTC (ER sometimes returns offset-less ISO strings).
    Otherwise convert to UTC so comparisons are always tz-homogeneous.
    """
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _parse_dt(value):
    if not value:
        return None
    # ER serializes assigned_range bounds with a trailing 'Z'
    # (e.g. "9999-12-31T23:59:59.999999Z"), which datetime.fromisoformat
    # rejects on Python 3.10. Normalize to a +00:00 offset before parsing.
    if isinstance(value, str) and value.endswith("Z"):
        value = value[:-1] + "+00:00"
    return _ensure_utc(datetime.fromisoformat(value))


class Assignment(BaseModel):
    lower: datetime
    upper: Optional[datetime] = None  # None = still assigned (open-ended)
    subject_name: Optional[str] = None
    subject_type: Optional[str] = None

    def covers(self, when: datetime) -> bool:
        when = _ensure_utc(when)
        if when is None:
            return False
        lower = _ensure_utc(self.lower)
        upper = _ensure_utc(self.upper)
        if when < lower:
            return False
        if upper is not None and when >= upper:  # half-open [lower, upper)
            return False
        return True


class SourceProfile(BaseModel):
    manufacturer_id: Optional[str] = None
    assignments: List[Assignment] = []


class ResolvedSource(BaseModel):
    external_source_id: str
    source_name: Optional[str] = None
    subject_type: Optional[str] = None


def _chunked(seq, size):
    seq = list(seq)
    for i in range(0, len(seq), size):
        yield seq[i:i + size]


def resolve_source(profile: Optional[SourceProfile], source_uuid: str, recorded_at: datetime) -> ResolvedSource:
    """Pure: map a source UUID + observation time to its Gundi identifiers.

    external_source_id = manufacturer_id, falling back to er-src-{uuid}. The
    subject name/type come from the assignment whose half-open [lower, upper)
    range contains recorded_at; on overlap the latest-starting assignment wins.
    """
    fallback = f"er-src-{source_uuid}"
    if profile is None:
        return ResolvedSource(external_source_id=fallback)
    external = profile.manufacturer_id or fallback
    covering = [a for a in profile.assignments if a.covers(recorded_at)]
    chosen = max(covering, key=lambda a: _ensure_utc(a.lower)) if covering else None
    return ResolvedSource(
        external_source_id=external,
        source_name=chosen.subject_name if chosen else None,
        subject_type=chosen.subject_type if chosen else None,
    )


class SourceProfileResolver:
    """Per-run resolver: source UUID -> SourceProfile, fetched lazily and cached.

    Covers both pull paths because it keys off the source UUIDs that actually
    appear in observations. Enrichment failures degrade to a UUID fallback and
    are never allowed to fail the pull.
    """

    def __init__(self, er_client, *, integration_id=None):
        self._er = er_client
        self._integration_id = integration_id
        self._cache = {}

    async def ensure(self, source_uuids: Iterable[str]) -> None:
        missing = sorted({u for u in source_uuids if u and u not in self._cache})
        if not missing:
            return
        # _fetch_ranges is now per-chunk resilient and never raises; always returns
        # whatever it managed to collect before any failing chunk.
        ranges_by_source = await self._fetch_ranges(missing)
        for uuid in missing:
            try:
                self._cache[uuid] = await self._build_profile(uuid, ranges_by_source.get(uuid, []))
            except Exception as e:
                logger.warning(
                    "Source profile fetch failed for %s (%s); using UUID fallback.",
                    uuid, e, extra={"attention_needed": True},
                )
                self._cache[uuid] = SourceProfile()

    def resolve(self, source_uuid, recorded_at) -> ResolvedSource:
        return resolve_source(self._cache.get(source_uuid), source_uuid, recorded_at)

    async def _fetch_ranges(self, source_uuids):
        """source UUID -> list of (lower, upper, subject_uuid) from /subjectsources.

        Chunks the request to keep URLs short. Per-chunk failures are logged and
        skipped; successfully collected chunks are always returned. The fallback
        to an empty SourceProfile() happens in ensure() for sources whose UUID
        is absent from the returned dict.
        """
        out = {}
        for chunk in _chunked(source_uuids, SOURCE_ID_CHUNK_SIZE):
            try:
                raw = await self._er.get_source_assignments(source_ids=list(chunk))
            except Exception as e:
                logger.warning(
                    "Source assignments chunk fetch failed for %d sources (%s); "
                    "skipping chunk, already-collected results are preserved.",
                    len(chunk), e, extra={"attention_needed": True},
                )
                continue
            if isinstance(raw, dict):
                if raw.get("next"):
                    logger.warning(
                        "subjectsources chunk returned a paginated 'next' "
                        "(count=%s, chunk_size=%d) — sources may be dropped; "
                        "lower SOURCE_ID_CHUNK_SIZE.",
                        raw.get("count"), len(chunk),
                        extra={"attention_needed": True},
                    )
                records = raw.get("results", [])
            else:
                records = raw
            for rec in records or []:
                src = rec.get("source")
                rng = rec.get("assigned_range") or {}
                if not src:
                    continue
                out.setdefault(src, []).append(
                    (_parse_dt(rng.get("lower")), _parse_dt(rng.get("upper")), rec.get("subject"))
                )
        return out

    async def _build_profile(self, source_uuid, ranges):
        # ER's get_source_by_manufacturer_id GETs /source/{id}/ — despite the method
        # name it resolves by source PK (UUID), not manufacturer_id. No clearer method
        # exists on AsyncERClient; see: dir(erclient.AsyncERClient) → no get_source/get_source_by_id.
        detail = await self._er.get_source_by_manufacturer_id(source_uuid)
        manufacturer_id = (detail or {}).get("manufacturer_id")
        subjects = await self._er.get_source_subjects(source_uuid)
        subj_by_id = {s.get("id"): s for s in (subjects or [])}
        assignments = []
        for lower, upper, subject_uuid in ranges:
            if lower is None:
                continue
            s = subj_by_id.get(subject_uuid, {})
            assignments.append(Assignment(
                lower=lower, upper=upper,
                subject_name=s.get("name"),
                subject_type=s.get("subject_type"),
            ))
        return SourceProfile(manufacturer_id=manufacturer_id, assignments=assignments)
