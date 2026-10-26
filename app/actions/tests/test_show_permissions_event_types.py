import pytest
from unittest.mock import AsyncMock, MagicMock

from erclient import VERSION_1_0, VERSION_2_0

from app.actions.handlers import (
    _fetch_all_event_types_v1_shape,
    _merge_event_categories_and_type_perms,
)


V1_TYPES = [
    {
        "value": "accident_rep",
        "display": "Accident Report",
        "category": {"value": "monitoring", "display": "Monitoring", "is_active": True},
    },
]
V2_TYPES = [
    {"value": "wildlife_sighting", "display": "Wildlife Sighting", "category": "monitoring"},
    {"value": "fire", "display": "Fire", "category": "security"},
]
CATEGORIES = [
    {"value": "monitoring", "display": "Monitoring"},
    {"value": "security", "display": "Security"},
]


def _er_client(fail_versions=()):
    async def get_event_types(version=VERSION_1_0, **kwargs):
        if version in fail_versions:
            raise RuntimeError(f"{version} unavailable")
        return V1_TYPES if version == VERSION_1_0 else V2_TYPES

    client = MagicMock()
    client.get_event_types = AsyncMock(side_effect=get_event_types)
    client.get_event_categories = AsyncMock(return_value=CATEGORIES)
    return client


@pytest.mark.asyncio
async def test_show_permissions_lists_v1_and_v2_event_types():
    event_types = await _fetch_all_event_types_v1_shape(_er_client())

    categories = _merge_event_categories_and_type_perms(
        global_category_permissions={"security": ["read"]},
        er_event_types=event_types,
    )
    assert categories == {
        "Monitoring": {"Event Types": ["Accident Report", "Wildlife Sighting"], "Permissions": ["view"]},
        "Security": {"Event Types": ["Fire"], "Permissions": ["read"]},
    }


@pytest.mark.asyncio
async def test_show_permissions_keeps_v2_types_when_v1_fails():
    event_types = await _fetch_all_event_types_v1_shape(_er_client(fail_versions=(VERSION_1_0,)))

    assert sorted(et["value"] for et in event_types) == ["fire", "wildlife_sighting"]


@pytest.mark.asyncio
async def test_show_permissions_keeps_v1_types_when_v2_fails():
    client = _er_client(fail_versions=(VERSION_2_0,))

    event_types = await _fetch_all_event_types_v1_shape(client)

    assert [et["value"] for et in event_types] == ["accident_rep"]
    assert not client.get_event_categories.called


@pytest.mark.asyncio
async def test_show_permissions_raises_when_both_versions_fail():
    with pytest.raises(RuntimeError):
        await _fetch_all_event_types_v1_shape(_er_client(fail_versions=(VERSION_1_0, VERSION_2_0)))


@pytest.mark.asyncio
async def test_show_permissions_uses_one_category_name_for_v1_and_v2():
    client = _er_client()
    client.get_event_categories = AsyncMock(
        return_value=[{"value": "monitoring", "display": "Monitoring & Patrols"}]
    )

    event_types = await _fetch_all_event_types_v1_shape(client)

    categories = _merge_event_categories_and_type_perms(
        global_category_permissions={}, er_event_types=event_types,
    )
    assert categories["Monitoring & Patrols"]["Event Types"] == ["Accident Report", "Wildlife Sighting"]
    assert "Monitoring" not in categories


@pytest.mark.asyncio
async def test_show_permissions_falls_back_to_slug_when_categories_fail():
    client = _er_client()
    client.get_event_categories = AsyncMock(side_effect=RuntimeError("categories unavailable"))

    event_types = await _fetch_all_event_types_v1_shape(client)

    fire = next(et for et in event_types if et["value"] == "fire")
    assert fire["category"] == {"value": "security", "display": "security", "is_active": True}
    # The v1 type names "monitoring", so the v2 type in it still groups with it.
    categories = _merge_event_categories_and_type_perms(
        global_category_permissions={}, er_event_types=event_types,
    )
    assert categories["Monitoring"]["Event Types"] == ["Accident Report", "Wildlife Sighting"]
    assert "monitoring" not in categories
