"""Tests for the three ER provider-side reference actions: list_event_types,
list_event_type_fields, and list_event_field_values. AsyncERClient and
fetch_prerendered_event_schema are mocked at the app.actions.handlers module
level; parse_er_event_schema runs for real against the captured rhino_carcass
fixture so these tests also exercise the schema-driven handlers end to end.
"""

import json
import os
from unittest.mock import AsyncMock, MagicMock

import httpx
import pytest

from app.actions.configurations import (
    ListEventTypesQuery,
    ListEventTypeFieldsQuery,
    ListEventFieldValuesQuery,
)
from app.actions.handlers import (
    action_list_event_types,
    action_list_event_type_fields,
    action_list_event_field_values,
)

_FIXTURE_PATH = os.path.join(
    os.path.dirname(__file__), "fixtures", "rhino_carcass_schema_from_api.json"
)


def _load_fixture():
    with open(_FIXTURE_PATH) as fh:
        return json.load(fh)


@pytest.fixture
def mock_er_client(mocker):
    """Patch AsyncERClient in handlers; returns the mock instance the
    `async with AsyncERClient(...) as earth_ranger:` block yields."""
    from app.actions import handlers as handlers_module

    instance = MagicMock()
    instance.get_event_types = AsyncMock(return_value=[])
    instance.get_event_categories = AsyncMock(return_value=[])
    client_cls = MagicMock()
    client_cls.return_value.__aenter__ = AsyncMock(return_value=instance)
    client_cls.return_value.__aexit__ = AsyncMock(return_value=False)
    mocker.patch.object(handlers_module, "AsyncERClient", client_cls)
    return instance


@pytest.fixture
def mock_fetch_schema(mocker):
    """Patch fetch_prerendered_event_schema in handlers (Task 2's fetch
    helper) so list_event_type_fields/list_event_field_values tests don't
    depend on real ER schema HTTP calls."""
    from app.actions import handlers as handlers_module

    mock = AsyncMock()
    mocker.patch.object(handlers_module, "fetch_prerendered_event_schema", mock)
    return mock


# --- action_list_event_types -------------------------------------------------


@pytest.mark.asyncio
async def test_list_event_types_groups_by_category(
    er_integration_v2_provider, mock_er_client
):
    """v1 and v2 event types are merged; options are grouped by category
    display name (from v1's nested category dict, including for a v2-only
    type sharing that category slug) and sorted by group then label."""
    from erclient import VERSION_1_0, VERSION_2_0

    async def fake_get_event_types(version=VERSION_1_0, **kwargs):
        if version == VERSION_1_0:
            return [
                {
                    "value": "rhino_carcass", "display": "Rhino Carcass", "id": "id1",
                    "category": {"value": "wildlife", "id": "catw", "display": "Wildlife"},
                },
                {
                    "value": "accident_rep", "display": "Accident Report", "id": "id2",
                    "category": {"value": "security", "id": "cats", "display": "Security"},
                },
            ]
        if version == VERSION_2_0:
            return [
                # v2-only type sharing the "wildlife" category slug seen from v1.
                {"value": "coyote_carcass", "display": "Coyote Carcass", "id": "id3", "category": "wildlife"},
            ]
        return []

    mock_er_client.get_event_types = fake_get_event_types

    result = await action_list_event_types(er_integration_v2_provider, ListEventTypesQuery())

    values = [(o["value"], o["label"], o["group"]) for o in result["options"]]
    assert values == [
        ("accident_rep", "Accident Report", "Security"),
        ("coyote_carcass", "Coyote Carcass", "Wildlife"),
        ("rhino_carcass", "Rhino Carcass", "Wildlife"),
    ]
    assert result["truncated"] is False


@pytest.mark.asyncio
async def test_list_event_types_ungrouped_when_no_category_display_known(
    er_integration_v2_provider, mock_er_client
):
    """A v2-only type whose category slug never resolved to a display name
    (get_event_categories also came up empty) gets no group rather than an
    error."""
    from erclient import VERSION_1_0, VERSION_2_0

    async def fake_get_event_types(version=VERSION_1_0, **kwargs):
        if version == VERSION_1_0:
            return []
        if version == VERSION_2_0:
            return [{"value": "lone_type", "display": "Lone Type", "id": "id1", "category": "mystery"}]
        return []

    mock_er_client.get_event_types = fake_get_event_types
    mock_er_client.get_event_categories = AsyncMock(return_value=[])

    result = await action_list_event_types(er_integration_v2_provider, ListEventTypesQuery())

    assert result["options"] == [
        {"value": "lone_type", "label": "Lone Type", "description": None, "group": None}
    ]


# --- action_list_event_type_fields ------------------------------------------


@pytest.mark.asyncio
async def test_list_event_type_fields_returns_options_from_schema(
    er_integration_v2_provider, mock_er_client, mock_fetch_schema
):
    mock_fetch_schema.return_value = _load_fixture()

    result = await action_list_event_type_fields(
        er_integration_v2_provider, ListEventTypeFieldsQuery(event_type="rhino_carcass")
    )

    by_value = {o["value"]: o for o in result["options"]}
    assert [o["value"] for o in result["options"]] == [
        "cause_of_death", "age_of_carcass", "animal_sex", "animal_id",
        "age_of_animal", "animal_common_name", "reported_to_opswpu",
    ]
    assert by_value["animal_sex"]["label"] == "Animal Sex"
    assert by_value["animal_sex"]["description"] == "choices"
    assert by_value["animal_id"]["label"] == "Animal ID"
    assert by_value["animal_id"]["description"] is None
    mock_fetch_schema.assert_awaited_once_with(
        mock_er_client, er_integration_v2_provider.base_url, "rhino_carcass"
    )


@pytest.mark.asyncio
async def test_list_event_type_fields_unknown_event_type_raises(
    er_integration_v2_provider, mock_er_client, mock_fetch_schema
):
    mock_fetch_schema.side_effect = httpx.HTTPStatusError(
        "Not Found", request=MagicMock(), response=MagicMock(status_code=404)
    )

    with pytest.raises(ValueError, match="rhino_carcass.*not found in EarthRanger"):
        await action_list_event_type_fields(
            er_integration_v2_provider, ListEventTypeFieldsQuery(event_type="rhino_carcass")
        )


@pytest.mark.asyncio
async def test_list_event_type_fields_upstream_500_propagates(
    er_integration_v2_provider, mock_er_client, mock_fetch_schema
):
    """A non-404 upstream failure (e.g. a 500) must propagate, not be
    swallowed or mistranslated into the 404 ValueError."""
    mock_fetch_schema.side_effect = httpx.HTTPStatusError(
        "Server Error", request=MagicMock(), response=MagicMock(status_code=500)
    )

    with pytest.raises(httpx.HTTPStatusError):
        await action_list_event_type_fields(
            er_integration_v2_provider, ListEventTypeFieldsQuery(event_type="rhino_carcass")
        )


# --- action_list_event_field_values -----------------------------------------


@pytest.mark.asyncio
async def test_list_event_field_values_returns_choices_for_enum_field(
    er_integration_v2_provider, mock_er_client, mock_fetch_schema
):
    mock_fetch_schema.return_value = _load_fixture()

    result = await action_list_event_field_values(
        er_integration_v2_provider,
        ListEventFieldValuesQuery(event_type="rhino_carcass", field_key="animal_sex"),
    )

    assert [(o["value"], o["label"]) for o in result["options"]] == [
        ("female", "Female"),
        ("male", "Male"),
        ("Unknown", "Unknown"),
    ]


@pytest.mark.asyncio
async def test_list_event_field_values_non_enum_field_returns_empty(
    er_integration_v2_provider, mock_er_client, mock_fetch_schema
):
    """animal_id is free-text in the fixture (no inline choices) — matches
    CMORE's list_field_options non-lookup behavior: empty options, not an
    error."""
    mock_fetch_schema.return_value = _load_fixture()

    result = await action_list_event_field_values(
        er_integration_v2_provider,
        ListEventFieldValuesQuery(event_type="rhino_carcass", field_key="animal_id"),
    )

    assert result["options"] == []


@pytest.mark.asyncio
async def test_list_event_field_values_unknown_field_raises(
    er_integration_v2_provider, mock_er_client, mock_fetch_schema
):
    mock_fetch_schema.return_value = _load_fixture()

    with pytest.raises(ValueError, match="no_such_field"):
        await action_list_event_field_values(
            er_integration_v2_provider,
            ListEventFieldValuesQuery(event_type="rhino_carcass", field_key="no_such_field"),
        )


@pytest.mark.asyncio
async def test_list_event_field_values_unknown_event_type_raises(
    er_integration_v2_provider, mock_er_client, mock_fetch_schema
):
    mock_fetch_schema.side_effect = httpx.HTTPStatusError(
        "Not Found", request=MagicMock(), response=MagicMock(status_code=404)
    )

    with pytest.raises(ValueError, match="not found in EarthRanger"):
        await action_list_event_field_values(
            er_integration_v2_provider,
            ListEventFieldValuesQuery(event_type="no_such_type", field_key="animal_sex"),
        )
