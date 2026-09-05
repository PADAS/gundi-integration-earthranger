"""Parse an EarthRanger pre-rendered event-type schema into a flat list of fields.

Ported from ``gundi-integration-cmore/app/datasource/er_schema.py`` (CMORE's
mapping-scaffold tooling) for use by this repo's provider-side reference
actions (``list_event_type_fields`` / ``list_event_field_values``).

ER's v2 event-type schema is JSON Schema (draft 2020-12), wrapped in a
top-level ``json`` key (with a sibling ``ui`` block). Each field lives under
``json.properties`` with a ``title``. When fetched with
``pre_render=true&s_format=enum`` (see ``fetch_prerendered_event_schema``),
choice fields carry their values inline as an ``enum`` (+ ``x-enumExtra``
display names) nested in an ``anyOf`` member::

    "animal_sex": {
        "title": "Animal Sex", "type": "string",
        "anyOf": [{"type": "string", "enum": ["female", "male"],
                    "x-enumExtra": {"female": {"display": "Female"}, ...}}]
    }

CMORE's original parser also handled schemas *without* ``pre_render``, where
choice fields instead carry a ``$ref`` to an external choices.json list that
must be fetched separately (``choices_ref`` / ``choice_list`` /
``parse_choices_json``). This repo only ever calls the pre-rendered endpoint,
and every choice field observed in the captured fixture
(``app/actions/tests/fixtures/rhino_carcass_schema_from_api.json``) is fully
inlined —
so that ref-resolution machinery is dropped here as dead code for this call
site. Restore it from the CMORE source if a future field is found that isn't
inlined even under pre_render.
"""

from dataclasses import dataclass
from typing import List, Optional
from urllib.parse import quote, urlparse

import httpx

from app.services.errors import IntegrationConfigurationError


@dataclass
class ERChoice:
    value: str
    display: str


@dataclass
class ERField:
    key: str
    title: str
    choices: Optional[List[ERChoice]] = None  # populated when this is a choice field

    @property
    def is_enum(self) -> bool:
        """True if this is a choice field."""
        return self.choices is not None


def _locate_schema(raw: dict) -> dict:
    """Descend through ER's ``json``/``schema``/``data`` envelopes to the
    object that actually holds ``properties``."""
    node = raw
    for _ in range(6):  # bounded descent
        if not isinstance(node, dict):
            return {}
        if isinstance(node.get("properties"), dict):
            return node
        for key in ("json", "schema", "data"):
            if isinstance(node.get(key), dict):
                node = node[key]
                break
        else:
            return {}
    return node if isinstance(node, dict) else {}


def _enum_with_extra(node: dict) -> Optional[List[ERChoice]]:
    """Parse an ``enum`` (+ optional ``enumNames`` / ``x-enumExtra``) block.

    ER's pre-rendered ``s_format=enum`` schema carries choices as
    ``{"enum": [...], "x-enumExtra": {value: {"display": ...}}}``.
    """
    enum = node.get("enum")
    if not isinstance(enum, list) or not enum:
        return None
    extra = node.get("x-enumExtra")
    names = node.get("enumNames")
    out = []
    for value in enum:
        display = value
        if isinstance(extra, dict) and isinstance(extra.get(value), dict):
            display = extra[value].get("display", value)
        elif isinstance(names, dict):
            display = names.get(value, value)
        out.append(ERChoice(str(value), str(display)))
    if isinstance(names, list) and len(names) == len(enum):
        out = [ERChoice(str(v), str(label)) for v, label in zip(enum, names)]
    return out


def _inline_choices(prop: dict) -> Optional[List[ERChoice]]:
    """Choices carried directly on the property: a top-level ``enum``, an
    ``enum`` nested in an ``anyOf``/``oneOf`` member (ER's pre-rendered form),
    or a ``oneOf``/``anyOf`` list of ``{const/value, title/display}``."""
    direct = _enum_with_extra(prop)
    if direct is not None:
        return direct

    for key in ("anyOf", "oneOf"):
        members = prop.get(key)
        if not isinstance(members, list):
            continue
        for member in members:
            if not isinstance(member, dict):
                continue
            nested = _enum_with_extra(member)
            if nested is not None:
                return nested
        # const/value-style choice members (no enum).
        dict_members = [m for m in members if isinstance(m, dict)]
        if dict_members and all(("const" in m or "value" in m) for m in dict_members):
            return [
                ERChoice(
                    str(m.get("const", m.get("value"))),
                    str(m.get("title", m.get("display", m.get("const", m.get("value"))))),
                )
                for m in dict_members
            ]
    return None


def parse_er_event_schema(raw: dict) -> List[ERField]:
    """Parse a pre-rendered ER event-type schema response into a flat list of
    ``ERField``. Fields with no inline choices are free-text. Order follows
    the schema's ``properties`` insertion order.
    """
    schema = _locate_schema(raw or {})
    props = schema.get("properties") if isinstance(schema, dict) else None
    if not isinstance(props, dict):
        return []
    fields: List[ERField] = []
    for key, prop in props.items():
        if not isinstance(prop, dict):
            continue
        title = str(prop.get("title") or key)
        fields.append(ERField(
            key=key,
            title=title,
            choices=_inline_choices(prop),
        ))
    return fields


def er_site_root(base_url: str) -> str:
    """`scheme://hostname` of an EarthRanger site URL (dropping any port), the
    convention every AsyncERClient construction site uses.

    Schemeless or empty base_urls do occur in Gundi (see the _ensure_scheme
    guard in gundi-integration-cmore's CLI); urlparse leaves hostname None for
    them, which would otherwise build a "https://None/..." URL. That is a
    configuration error, and the URL itself stays out of the message: it is a
    submitted value that may carry a token in its path or query.
    """
    url_parse = urlparse(base_url or "")
    if not url_parse.hostname:
        raise IntegrationConfigurationError("Site URL is empty or invalid.")
    return f"{url_parse.scheme}://{url_parse.hostname}"


async def fetch_prerendered_event_schema(er_client, base_url: str, event_type: str) -> dict:
    """GET the v2 pre-rendered event-type schema (pre_render + s_format=enum inline
    each choice field's values, so no separate choices fetch is needed). erclient
    doesn't model this /schema sub-resource, so we call it directly with the
    client's own auth headers (works for both token and username/password auth)."""
    url = f"{er_site_root(base_url)}/api/v2.0/activity/eventtypes/{quote(event_type, safe='')}/schema"
    try:
        headers = await er_client.auth_headers()
    except httpx.HTTPStatusError as e:
        # auth_headers() runs outside erclient's _call wrapper here, so a login
        # failure (the token endpoint's 400 invalid_grant, or a 401) would
        # escape as a raw httpx error carrying the login request body, which
        # for a username/password integration is the password. Apply the same
        # mapping _call does, so it comes out as an ERClientException like
        # every other failure and is translated downstream.
        er_client._handle_http_status_error("oauth2/token", "POST", e, request_url=str(e.request.url))
    async with httpx.AsyncClient(headers=headers, timeout=30.0) as http:
        resp = await http.get(url, params={"pre_render": "true", "s_format": "enum"})
        resp.raise_for_status()
        return resp.json()
