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


def _er(class_name, status_code):
    """Build an erclient exception the way the client does, response body included."""
    from erclient import er_errors

    return getattr(er_errors, class_name)("ER said no", status_code=status_code, response_body="secret-body")


def _load_fixture():
    with open(_FIXTURE_PATH) as fh:
        return json.load(fh)


@pytest.fixture
def mock_er_client(mocker):
    """Patch AsyncERClient in handlers; returns the mock instance the
    `async with AsyncERClient(...) as earth_ranger:` block yields.
    The instance is autospecced from the real installed client, so a
    handler passing kwargs the pinned erclient doesn't support fails
    here with a TypeError instead of being masked by the mock."""
    from erclient import AsyncERClient
    from app.actions import handlers as handlers_module

    instance = mocker.create_autospec(AsyncERClient, instance=True)
    instance.get_event_types.return_value = []
    instance.get_event_categories.return_value = []
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
    """Only v2 event types are offered (see
    test_list_event_types_excludes_v1_only_types), grouped by category
    display name and sorted by group then label. Category display names can
    come from either version's response — here both groups are known via
    v1's nested category dict."""
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
                # v2-only types, one sharing the "wildlife" category slug seen from v1.
                {"value": "coyote_carcass", "display": "Coyote Carcass", "id": "id3", "category": "wildlife"},
                {"value": "sit_rep_v2", "display": "Sit Rep", "id": "id4", "category": "security"},
            ]
        return []

    mock_er_client.get_event_types = fake_get_event_types
    mock_er_client.get_event_categories = AsyncMock(return_value=[])

    result = await action_list_event_types(er_integration_v2_provider, ListEventTypesQuery())

    values = [(o["value"], o["label"], o["group"]) for o in result["options"]]
    assert values == [
        ("sit_rep_v2", "Sit Rep", "Security"),
        ("coyote_carcass", "Coyote Carcass", "Wildlife"),
    ]
    assert result["truncated"] is False


@pytest.mark.asyncio
async def test_list_event_types_excludes_v1_only_types(
    er_integration_v2_provider, mock_er_client
):
    """Classic v1 event types dead-end the list_event_type_fields /
    list_event_field_values cascade (ER's v2 schema endpoint 404s for them),
    so list_event_types must not offer them at all — only v2-sourced slugs
    appear, even though the v1 type is known (it contributes display_by_slug
    and category info, just not an option)."""
    from erclient import VERSION_1_0, VERSION_2_0

    async def fake_get_event_types(version=VERSION_1_0, **kwargs):
        if version == VERSION_1_0:
            return [{"value": "rhino_carcass", "display": "Rhino Carcass", "id": "id1", "category": {}}]
        if version == VERSION_2_0:
            return [{"value": "coyote_carcass", "display": "Coyote Carcass", "id": "id2", "category": {}}]
        return []

    mock_er_client.get_event_types = fake_get_event_types

    result = await action_list_event_types(er_integration_v2_provider, ListEventTypesQuery())

    assert [o["value"] for o in result["options"]] == ["coyote_carcass"]


@pytest.mark.asyncio
async def test_list_event_types_group_resolves_via_unconditional_categories_fetch(
    er_integration_v2_provider, mock_er_client
):
    """A v1 event type already populates category_id_by_slug, which would
    normally suppress `_fetch_event_type_maps`'s own categories-endpoint
    fallback (that fallback is tuned for pull_events' UUID-resolution need,
    not display names). list_event_types must still resolve display names
    for categories only seen via v2 — via its own unconditional
    get_event_categories fetch — rather than leaving them ungrouped."""
    from erclient import VERSION_1_0, VERSION_2_0

    async def fake_get_event_types(version=VERSION_1_0, **kwargs):
        if version == VERSION_1_0:
            return [
                {
                    "value": "rhino_carcass", "display": "Rhino Carcass", "id": "id1",
                    "category": {"value": "wildlife", "id": "catw", "display": "Wildlife"},
                },
            ]
        if version == VERSION_2_0:
            # v2-only type in a category v1 never mentioned.
            return [{"value": "lone_type", "display": "Lone Type", "id": "id2", "category": "mystery"}]
        return []

    mock_er_client.get_event_types = fake_get_event_types
    mock_er_client.get_event_categories = AsyncMock(
        return_value=[{"value": "mystery", "id": "catm", "display": "Mystery"}]
    )

    result = await action_list_event_types(er_integration_v2_provider, ListEventTypesQuery())

    # rhino_carcass (v1-only) is excluded; lone_type's group resolved to
    # "Mystery" via the unconditional fetch even though category_id_by_slug
    # was already non-empty from v1.
    assert result["options"] == [
        {"value": "lone_type", "label": "Lone Type", "description": None, "group": "Mystery"}
    ]
    mock_er_client.get_event_categories.assert_awaited_once()


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

    # A connector guard the portal wizard must be able to act on: the runner
    # forwards IntegrationConfigurationError on the ephemeral path (422),
    # where every other connector message is redacted to the type name. By
    # contract the message names the shape of the problem and never echoes
    # the submitted value.
    from app.services.errors import IntegrationConfigurationError

    with pytest.raises(IntegrationConfigurationError, match="v1 event type") as info:
        await action_list_event_type_fields(
            er_integration_v2_provider, ListEventTypeFieldsQuery(event_type="rhino_carcass")
        )
    assert "rhino_carcass" not in str(info.value)


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

    from app.services.errors import IntegrationConfigurationError

    with pytest.raises(IntegrationConfigurationError, match="Field not found") as info:
        await action_list_event_field_values(
            er_integration_v2_provider,
            ListEventFieldValuesQuery(event_type="rhino_carcass", field_key="no_such_field"),
        )
    assert "no_such_field" not in str(info.value)


@pytest.mark.asyncio
async def test_list_event_field_values_unknown_event_type_raises(
    er_integration_v2_provider, mock_er_client, mock_fetch_schema
):
    mock_fetch_schema.side_effect = httpx.HTTPStatusError(
        "Not Found", request=MagicMock(), response=MagicMock(status_code=404)
    )

    from app.services.errors import IntegrationConfigurationError

    with pytest.raises(IntegrationConfigurationError, match="v1 event type") as info:
        await action_list_event_field_values(
            er_integration_v2_provider,
            ListEventFieldValuesQuery(event_type="no_such_type", field_key="animal_sex"),
        )
    assert "no_such_type" not in str(info.value)


# --- base_url validation in _build_er_client ----------------------------------


@pytest.mark.asyncio
async def test_reference_action_with_schemeless_base_url_raises_clean_error(
    mocker, er_integration_v2_provider
):
    """A schemeless/empty integration.base_url must fail with the same clean
    'Site URL is empty or invalid' wording action_auth uses, not a confusing
    downstream error against https://None/... As a configuration error it
    reaches the portal wizard on the ephemeral path; the draft URL itself is
    a submitted value and stays out of the message."""
    from app.actions.handlers import action_list_event_types
    from app.actions.configurations import ListEventTypesQuery
    from app.services.errors import IntegrationConfigurationError

    mocker.patch.object(er_integration_v2_provider, "base_url", "gundi-er.pamdas.org")
    with pytest.raises(IntegrationConfigurationError, match="Site URL is empty or invalid") as info:
        await action_list_event_types(er_integration_v2_provider, ListEventTypesQuery())
    assert "gundi-er.pamdas.org" not in str(info.value)


@pytest.mark.asyncio
async def test_reference_action_without_auth_settings_raises_a_configuration_error(er_integration_v2_provider):
    """The wizard can open a dropdown before the auth step is saved. Missing
    auth settings are a configuration problem, not an EarthRanger verdict, and
    the message must not carry the integration id (an identifier the draft
    path synthesizes per run)."""
    from app.actions.handlers import action_list_event_types
    from app.actions.configurations import ListEventTypesQuery
    from app.services.errors import IntegrationConfigurationError

    integration = er_integration_v2_provider.copy(update={
        "configurations": [c for c in er_integration_v2_provider.configurations if c.action.value != "auth"],
    })
    with pytest.raises(IntegrationConfigurationError, match="Authentication settings are missing") as info:
        await action_list_event_types(integration, ListEventTypesQuery())
    assert str(integration.id) not in str(info.value)


# --- action_list_event_categories ---------------------------------------------


@pytest.mark.asyncio
async def test_list_event_categories_returns_sorted_options(mock_er_client, er_integration_v2_provider):
    from app.actions.handlers import action_list_event_categories
    from app.actions.configurations import ListEventCategoriesQuery

    mock_er_client.get_event_categories = AsyncMock(return_value=[
        {"value": "security", "display": "Security"},
        {"value": "monitoring", "display": "Monitoring"},
        {"value": "no_display"},
        "not-a-dict",
    ])
    result = await action_list_event_categories(er_integration_v2_provider, ListEventCategoriesQuery())
    # Case-sensitive label sort, matching list_event_types' convention.
    assert [(o["value"], o["label"]) for o in result["options"]] == [
        ("monitoring", "Monitoring"),
        ("security", "Security"),
        ("no_display", "no_display"),
    ]
    assert result["truncated"] is False


@pytest.mark.asyncio
async def test_list_event_categories_unwraps_paginated_envelope(mock_er_client, er_integration_v2_provider):
    """ER can return the categories list wrapped in a paginated envelope
    ({"results": [...]}); the handler normalizes via _as_list."""
    from app.actions.handlers import action_list_event_categories
    from app.actions.configurations import ListEventCategoriesQuery

    mock_er_client.get_event_categories = AsyncMock(return_value={
        "results": [
            {"value": "security", "display": "Security"},
            {"value": "monitoring", "display": "Monitoring"},
        ]
    })
    result = await action_list_event_categories(er_integration_v2_provider, ListEventCategoriesQuery())
    assert [(o["value"], o["label"]) for o in result["options"]] == [
        ("monitoring", "Monitoring"),
        ("security", "Security"),
    ]


@pytest.mark.asyncio
async def test_list_event_categories_upstream_error_propagates(mock_er_client, er_integration_v2_provider):
    from app.actions.handlers import action_list_event_categories
    from app.actions.configurations import ListEventCategoriesQuery

    mock_er_client.get_event_categories = AsyncMock(side_effect=httpx.HTTPStatusError(
        "boom", request=MagicMock(), response=MagicMock(status_code=500)
    ))
    with pytest.raises(httpx.HTTPStatusError):
        await action_list_event_categories(er_integration_v2_provider, ListEventCategoriesQuery())


# --- PullEventsConfig ui_schema annotations ------------------------------------


def test_pull_events_ui_schema_annotates_slug_lists_with_references():
    """The slug-list fields render as reference dropdowns (per array item),
    without ever setting ui:widget (forward-compat with older portals)."""
    from app.actions.configurations import PullEventsConfig

    ui = PullEventsConfig.ui_schema()
    for field, action in (("event_types", "list_event_types"),
                          ("event_categories", "list_event_categories")):
        node = ui[field]["items"]
        ref = node["gundi:reference"]
        assert ref == {
            "action": action,
            "target": "self",
            "params": {},
            "allow_free_text": True,
        }
        assert "ui:widget" not in node
    # The generated order list is untouched.
    assert "event_types" in ui["ui:order"] and "event_categories" in ui["ui:order"]


# --- action_list_subject_types -------------------------------------------------


@pytest.mark.asyncio
async def test_list_subject_types_offers_observed_subtypes_grouped_by_type(
    er_integration_v2_provider, mock_er_client
):
    """Distinct observed subject_subtypes, grouped by subject_type and sorted
    by group; each subject_type is itself offered first in its group as a
    fallback option (the CMORE mapping matches subtype first, then type).
    Subjects appearing in several groups dedupe; subjects without a
    subject_subtype contribute only their type."""
    from app.actions.configurations import ListSubjectTypesQuery
    from app.actions.handlers import action_list_subject_types

    ranger = {"id": "s1", "name": "Ranger One",
              "subject_type": "person", "subject_subtype": "ranger"}
    # Configured on the autospecced method (not replaced with a bare
    # AsyncMock) so the call signature stays checked against the real client.
    mock_er_client.get_subjectgroups.return_value = [
        {"id": "g1", "name": "Rangers", "subjects": [
            ranger,
            {"id": "s2", "name": "Ranger Two",
             "subject_type": "person", "subject_subtype": "ranger"},
            {"id": "s3", "name": "Vet",
             "subject_type": "person", "subject_subtype": "veterinarian"},
        ]},
        {"id": "g2", "name": "Wildlife", "subjects": [
            {"id": "s4", "name": "Rhino A",
             "subject_type": "wildlife", "subject_subtype": "black_rhino"},
            ranger,  # same subject in a second group → dedupes
            {"id": "s5", "name": "Typed only", "subject_type": "wildlife"},
        ]},
    ]

    result = await action_list_subject_types(
        er_integration_v2_provider, ListSubjectTypesQuery()
    )

    options = [(o["value"], o["group"], o["description"]) for o in result["options"]]
    assert options == [
        ("person", "person", "Any 'person' subtype (fallback match)"),
        ("ranger", "person", None),
        ("veterinarian", "person", None),
        ("wildlife", "wildlife", "Any 'wildlife' subtype (fallback match)"),
        ("black_rhino", "wildlife", None),
    ]
    mock_er_client.get_subjectgroups.assert_awaited_once_with(
        include_inactive=True, flat=True
    )


@pytest.mark.asyncio
async def test_list_subject_types_subtype_wins_value_collision_and_handles_missing_type(
    er_integration_v2_provider, mock_er_client
):
    """A subtype slug equal to a type slug yields ONE option (the subtype);
    a subject with no subject_type still contributes its subtype, ungrouped."""
    from app.actions.configurations import ListSubjectTypesQuery
    from app.actions.handlers import action_list_subject_types

    mock_er_client.get_subjectgroups.return_value = [
        {"id": "g1", "subjects": [
            # Subtype literally named like its type.
            {"id": "s1", "subject_type": "vehicle", "subject_subtype": "vehicle"},
            # No subject_type at all.
            {"id": "s2", "subject_subtype": "mystery"},
        ]},
    ]

    result = await action_list_subject_types(
        er_integration_v2_provider, ListSubjectTypesQuery()
    )

    options = [(o["value"], o["group"], o["description"]) for o in result["options"]]
    assert options == [
        ("mystery", None, None),
        ("vehicle", "vehicle", None),
    ]


@pytest.mark.asyncio
async def test_list_subject_types_propagates_upstream_errors(
    er_integration_v2_provider, mock_er_client
):
    """The subjectgroups fetch is load-bearing — errors must propagate so the
    portal shows its "couldn't load options" degrade, never an empty list."""
    from app.actions.configurations import ListSubjectTypesQuery
    from app.actions.handlers import action_list_subject_types

    mock_er_client.get_subjectgroups.side_effect = httpx.HTTPStatusError(
        "boom", request=MagicMock(), response=MagicMock(status_code=502)
    )
    with pytest.raises(httpx.HTTPStatusError):
        await action_list_subject_types(
            er_integration_v2_provider, ListSubjectTypesQuery()
        )


# --- ER client errors on the reference path ------------------------------------


@pytest.mark.parametrize(
    "er_error,expected_type,expected_status",
    [
        (_er("ERClientBadCredentials", 401), "IntegrationAuthError", 401),
        (_er("ERClientPermissionDenied", 403), "IntegrationAuthError", 403),
        (_er("ERClientRateLimitExceeded", 429), "IntegrationRateLimitError", 429),
        (_er("ERClientInternalError", 500), "IntegrationBadResponseError", 500),
        (_er("ERClientNotFound", 404), "IntegrationBadResponseError", 404),
        (_er("ERClientServiceUnreachable", None), "IntegrationConnectionError", None),
    ],
)
@pytest.mark.asyncio
async def test_reference_actions_translate_er_client_errors(
        mock_er_client, er_integration_v2_provider, er_error, expected_type, expected_status,
):
    """erclient's exceptions are not IntegrationError subclasses and carry no
    .response, so the runner cannot classify them: on the ephemeral path the
    portal wizard got `500 {"error": "ERClientBadCredentials"}` and could not
    tell bad credentials from a broken site. Translated, it gets the runner's
    curated text and the source status (401/403/429/5xx)."""
    from app.services import errors
    from app.actions.handlers import action_list_event_categories
    from app.actions.configurations import ListEventCategoriesQuery

    mock_er_client.get_event_categories = AsyncMock(side_effect=er_error)
    with pytest.raises(getattr(errors, expected_type)) as info:
        await action_list_event_categories(er_integration_v2_provider, ListEventCategoriesQuery())

    assert info.value.status_code == expected_status
    assert info.value.__cause__ is er_error
    # The ER response body rides on str(ERClientException); it must not
    # reach the activity log or the portal through the translated message.
    assert "response_body" not in str(info.value) and "secret-body" not in str(info.value)


@pytest.mark.asyncio
async def test_list_event_types_surfaces_bad_credentials_instead_of_an_empty_list(
        mock_er_client, er_integration_v2_provider,
):
    """_fetch_event_type_maps is best-effort for pull_events (a 403 on one
    endpoint must not break the others), but on the reference path swallowing
    an auth failure returns 200 with no options: the wizard shows an empty
    dropdown instead of "Authentication failed"."""
    from app.services.errors import IntegrationAuthError

    mock_er_client.get_event_types = AsyncMock(side_effect=_er("ERClientBadCredentials", 401))
    with pytest.raises(IntegrationAuthError) as info:
        await action_list_event_types(er_integration_v2_provider, ListEventTypesQuery())
    assert info.value.status_code == 401
