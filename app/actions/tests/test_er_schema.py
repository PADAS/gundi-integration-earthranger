"""Tests for app.actions.er_schema: the pre-rendered event-type schema parser
and fetch helper consumed by the reference actions (list_event_type_fields,
list_event_field_values). ``parse_er_event_schema`` is exercised against a
real captured ER response (rhino_carcass_schema_from_api.json, copied from
gundi-integration-cmore/docs/) rather than a hand-built fixture."""

import json
import os
from unittest.mock import AsyncMock

import httpx
import pytest
import respx

from app.actions.er_schema import ERChoice, ERField, parse_er_event_schema, fetch_prerendered_event_schema

_FIXTURE_PATH = os.path.join(os.path.dirname(__file__), "fixtures", "rhino_carcass_schema_from_api.json")


def _load_fixture():
    with open(_FIXTURE_PATH) as fh:
        return json.load(fh)


# --- parse_er_event_schema against the real rhino_carcass fixture ----------


def test_parse_er_event_schema_returns_all_fields_in_order():
    fields = parse_er_event_schema(_load_fixture())
    assert [f.key for f in fields] == [
        "cause_of_death",
        "age_of_carcass",
        "animal_sex",
        "animal_id",
        "age_of_animal",
        "animal_common_name",
        "reported_to_opswpu",
    ]
    by_key = {f.key: f for f in fields}
    assert by_key["cause_of_death"].title == "Cause Of Death"
    assert by_key["animal_sex"].title == "Animal Sex"
    assert by_key["animal_id"].title == "Animal ID"


def test_parse_er_event_schema_inlines_choices_with_value_and_display():
    fields = parse_er_event_schema(_load_fixture())
    by_key = {f.key: f for f in fields}

    animal_sex = by_key["animal_sex"]
    assert animal_sex.is_enum
    assert animal_sex.choices == [
        ERChoice("female", "Female"),
        ERChoice("male", "Male"),
        ERChoice("Unknown", "Unknown"),
    ]

    cause_of_death = by_key["cause_of_death"]
    assert cause_of_death.is_enum
    assert ERChoice("gunshot", "Gunshot") in cause_of_death.choices
    assert ERChoice("oldage", "Old age") in cause_of_death.choices


def test_parse_er_event_schema_free_text_field_is_not_enum():
    fields = parse_er_event_schema(_load_fixture())
    by_key = {f.key: f for f in fields}
    assert by_key["animal_id"].is_enum is False
    assert by_key["animal_id"].choices is None


def test_parse_er_event_schema_empty_when_no_properties():
    assert parse_er_event_schema({"nonsense": True}) == []


def test_er_field_is_enum_true_only_when_choices_present():
    assert ERField(key="k", title="K", choices=[ERChoice("a", "A")]).is_enum is True
    assert ERField(key="k", title="K", choices=None).is_enum is False


# --- fetch_prerendered_event_schema -----------------------------------------


@pytest.mark.asyncio
@respx.mock
async def test_fetch_prerendered_event_schema_builds_url_and_forwards_auth():
    er_client = AsyncMock()
    er_client.auth_headers = AsyncMock(return_value={"Authorization": "Bearer x"})

    payload = {"json": {"properties": {}}}
    route = respx.get(
        "https://er.example.com/api/v2.0/activity/eventtypes/rhino_carcass/schema"
    ).mock(return_value=httpx.Response(200, json=payload))

    result = await fetch_prerendered_event_schema(
        er_client, base_url="https://er.example.com/api/v1.0", event_type="rhino_carcass"
    )

    assert result == payload
    assert route.called
    sent_request = route.calls.last.request
    assert sent_request.url.params["pre_render"] == "true"
    assert sent_request.url.params["s_format"] == "enum"
    assert sent_request.headers["Authorization"] == "Bearer x"
    er_client.auth_headers.assert_awaited_once()


@pytest.mark.asyncio
@respx.mock
async def test_fetch_prerendered_event_schema_url_encodes_event_type():
    er_client = AsyncMock()
    er_client.auth_headers = AsyncMock(return_value={"Authorization": "Bearer x"})

    route = respx.get(
        "https://er.example.com/api/v2.0/activity/eventtypes/rhino%2Fcarcass%20type/schema"
    ).mock(return_value=httpx.Response(200, json={}))

    await fetch_prerendered_event_schema(
        er_client, base_url="https://er.example.com/api/v1.0", event_type="rhino/carcass type"
    )

    assert route.called


@pytest.mark.asyncio
@respx.mock
async def test_fetch_prerendered_event_schema_raises_on_404():
    er_client = AsyncMock()
    er_client.auth_headers = AsyncMock(return_value={"Authorization": "Bearer x"})

    respx.get(
        "https://er.example.com/api/v2.0/activity/eventtypes/unknown_type/schema"
    ).mock(return_value=httpx.Response(404, json={"detail": "Not found"}))

    with pytest.raises(httpx.HTTPStatusError):
        await fetch_prerendered_event_schema(
            er_client, base_url="https://er.example.com", event_type="unknown_type"
        )


# --- base_url validation -----------------------------------------------------


@pytest.mark.asyncio
async def test_fetch_prerendered_event_schema_rejects_schemeless_base_url():
    # Schemeless base_urls occur in Gundi; urlparse leaves hostname None for
    # them, which would otherwise build a "https://None/..." URL.
    from app.services.errors import IntegrationConfigurationError

    er_client = AsyncMock()
    with pytest.raises(IntegrationConfigurationError, match="Site URL is empty or invalid") as info:
        await fetch_prerendered_event_schema(er_client, "gundi-er.pamdas.org", "rhino_carcass")
    assert "gundi-er.pamdas.org" not in str(info.value)  # a submitted value stays out of the message
    er_client.auth_headers.assert_not_awaited()


@pytest.mark.asyncio
async def test_fetch_prerendered_event_schema_rejects_empty_base_url():
    from app.services.errors import IntegrationConfigurationError

    er_client = AsyncMock()
    with pytest.raises(IntegrationConfigurationError, match="Site URL is empty or invalid"):
        await fetch_prerendered_event_schema(er_client, "", "rhino_carcass")


@pytest.mark.asyncio
async def test_fetch_prerendered_event_schema_maps_a_login_failure_like_the_client_does(er_400_invalid_credentials_exception):
    """fetch_prerendered_event_schema calls er_client.auth_headers() outside
    erclient's _call wrapper, so a username/password login failure escaped as a
    raw httpx.HTTPStatusError for the token URL. Nothing downstream translated
    it, and on a saved integration the runner would publish the request body,
    which for the token POST is the plaintext password. Route it through the
    client's own status-error mapping so it comes out as an ERClientException
    like every other failure."""
    from unittest.mock import AsyncMock
    from erclient import AsyncERClient
    from erclient.er_errors import ERClientBadRequest

    er_client = AsyncERClient(
        service_root="https://gundi-er.pamdas.org/api/v1.0", username="u", password="wrong",
        token_url="https://gundi-er.pamdas.org/oauth2/token", client_id="das_web_client",
    )
    er_client.auth_headers = AsyncMock(side_effect=er_400_invalid_credentials_exception)

    with pytest.raises(ERClientBadRequest) as info:
        await fetch_prerendered_event_schema(er_client, "https://gundi-er.pamdas.org", "rhino_carcass")
    assert info.value.status_code == 400
    assert "oauth2/token" in str(info.value)
