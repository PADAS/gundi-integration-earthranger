"""Spike: confirm the ER responses the SourceProfileResolver depends on.

Runs the three EarthRanger calls the resolver makes for one source UUID and
prints the JSON, so you can eyeball:

  * source detail        -> does it carry a populated `manufacturer_id`?
  * subjectsources        -> `assigned_range` shape + are HISTORICAL ranges returned?
  * source/{id}/subjects  -> subject `name` / `subject_type` for the join

The API *shapes* are already confirmed from the `das` serializers/viewsets
(see docs/superpowers/specs/2026-06-16-er-observation-source-naming-design.md);
this validates the live DATA for a real deployment and produces a fixture.

Usage:
    export ER_HOST=gundi-er.stage.pamdas.org     # hostname only, no scheme/path
    export ER_TOKEN=<an ER API token>
    export SOURCE_UUID=<a source UUID that has at least one subject assignment>
    # optional: export OUT=app/actions/tests/fixtures/subjectsources_sample.json
    python dev/spike_source_profile.py
"""
import asyncio
import json
import os

from erclient import AsyncERClient


async def main():
    host = os.environ["ER_HOST"]
    token = os.environ["ER_TOKEN"]
    source_uuid = os.environ["SOURCE_UUID"]

    client = AsyncERClient(
        service_root=f"https://{host}/api/v1.0",
        token=token,
        token_url=f"https://{host}/oauth2/token",
        client_id="das_web_client",
    )

    async with client as er:
        # 1) Source detail by UUID -> manufacturer_id. erclient.get_source_by_manufacturer_id
        #    GETs source/{identifier}/, which ER resolves by PK when given a UUID
        #    (das SourceView._resolve_identifier).
        source_detail = await er.get_source_by_manufacturer_id(source_uuid)
        # 2) Assignment history (ranges + subject UUIDs), filtered by source.
        assignments = await er.get_source_assignments(source_ids=[source_uuid])
        # 3) Subjects ever linked to the source (for the subject UUID -> name/type join).
        subjects = await er.get_source_subjects(source_uuid)

    result = {
        "source_uuid": source_uuid,
        "source_detail": source_detail,
        "assignments": assignments,
        "subjects": subjects,
    }
    print(json.dumps(result, indent=2, default=str))

    # Quick assertions to surface the answers without reading the full dump.
    mfg = (source_detail or {}).get("manufacturer_id") if isinstance(source_detail, dict) else None
    print("\n--- spike summary ---")
    print(f"manufacturer_id populated: {bool(mfg)} ({mfg!r})")
    recs = assignments.get("results", assignments) if isinstance(assignments, dict) else assignments
    print(f"assignment rows returned: {len(recs) if recs else 0}")
    if recs:
        print(f"first assigned_range: {recs[0].get('assigned_range')!r}")
        print(f"paginated 'next' present (rows may be truncated): "
              f"{bool(assignments.get('next')) if isinstance(assignments, dict) else False}")
    subj = subjects.get("results", subjects) if isinstance(subjects, dict) else subjects
    if subj:
        print(f"subject sample: name={subj[0].get('name')!r} subject_type={subj[0].get('subject_type')!r}")

    out = os.environ.get("OUT")
    if out:
        with open(out, "w") as f:
            json.dump(result, f, indent=2, default=str)
        print(f"\nwrote fixture: {out}")


if __name__ == "__main__":
    asyncio.run(main())
