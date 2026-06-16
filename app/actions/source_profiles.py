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
