# app/actions/source_profiles.py
import logging
from datetime import datetime
from typing import Iterable, List, Optional

from pydantic import BaseModel

logger = logging.getLogger(__name__)

# ER's /subjectsources accepts a comma-joined list of source UUIDs; keep chunks
# small so the query string can't 414 and the (single-page) response isn't truncated.
SOURCE_ID_CHUNK_SIZE = 25


class Assignment(BaseModel):
    lower: datetime
    upper: Optional[datetime] = None  # None = still assigned (open-ended)
    subject_name: Optional[str] = None
    subject_type: Optional[str] = None

    def covers(self, when: datetime) -> bool:
        if when < self.lower:
            return False
        if self.upper is not None and when >= self.upper:  # half-open [lower, upper)
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


def _parse_dt(value):
    from datetime import datetime
    return datetime.fromisoformat(value) if value else None


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
    chosen = max(covering, key=lambda a: a.lower) if covering else None
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
        try:
            ranges_by_source = await self._fetch_ranges(missing)
        except Exception as e:
            logger.warning(
                "Source assignments fetch failed for %s sources (%s); using UUID fallback for all.",
                len(missing), e, extra={"attention_needed": True},
            )
            for uuid in missing:
                self._cache[uuid] = SourceProfile()
            return
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
        """source UUID -> list of (lower, upper, subject_uuid) from /subjectsources."""
        out = {}
        for chunk in _chunked(source_uuids, SOURCE_ID_CHUNK_SIZE):
            try:
                raw = await self._er.get_source_assignments(source_ids=list(chunk))
            except Exception:
                raise  # handled per-source by ensure() fallback below
            records = raw.get("results", []) if isinstance(raw, dict) else raw
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
        detail = await self._er.get_source_by_manufacturer_id(source_uuid)  # source/{uuid}/
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
