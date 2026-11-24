"""The Share Events form: switches show the value the share uses, and the source name
boxes show only while a prefix that uses them is on."""
from app.actions.configurations import EventsCompatibility, SyncEventsConfig
from app.ersharing.modes import compatibility_overrides, event_settings

SWITCHES = ["prepend_system_to_categories", "prepend_system_to_event_types", "prepend_system_to_event_titles",
            "update_schema", "delete_unmatched_events"]
SITE = "https://serengeti.pamdas.org"


def _compat_schema() -> dict:
    return SyncEventsConfig.schema()["definitions"]["EventsCompatibility"]


def _matches(condition: dict, values: dict) -> bool:
    """A JSON Schema if-condition made of required consts, as the form checks it."""
    return all(f in values and values[f] == rule["const"] for f, rule in condition["properties"].items()) and \
        all(f in values for f in condition.get("required", []))


def _shown_boxes(values: dict) -> set:
    node, shown = _compat_schema(), set()
    while "if" in node:
        branch = node.get("then") if _matches(node["if"], values) else node.get("else")
        if not branch:
            break
        shown |= set(branch.get("properties", {}))
        node = branch
    return shown


def test_every_switch_shows_the_value_the_share_uses_when_left_alone():
    props = _compat_schema()["properties"]
    for switch in SWITCHES:
        assert props[switch]["default"] is True, switch
        assert props[switch]["description"], switch


def test_the_compatibility_section_is_a_plain_reference_so_the_form_fills_in_defaults():
    assert SyncEventsConfig.schema()["properties"]["compatibility"] == {
        "title": "Advanced (v1 compatibility)", "$ref": "#/definitions/EventsCompatibility",
    }


def test_the_source_name_boxes_follow_the_prefix_switches():
    on = {f: True for f in SWITCHES[:3]}
    assert _shown_boxes(on) == {"source_system_name", "source_system_value"}
    assert _shown_boxes({**on, "prepend_system_to_categories": False}) == {"source_system_name", "source_system_value"}
    titles_only = {**on, "prepend_system_to_categories": False, "prepend_system_to_event_types": False}
    assert _shown_boxes(titles_only) == {"source_system_name"}
    assert _shown_boxes({f: False for f in SWITCHES[:3]}) == set()


def test_the_boxes_sit_right_under_the_prefix_switches():
    assert SyncEventsConfig.ui_schema()["compatibility"]["ui:order"] == [
        "prepend_system_to_categories", "prepend_system_to_event_types", "prepend_system_to_event_titles",
        "*", "delete_unmatched_events", "update_schema",
    ]


def test_the_boxes_are_still_read_when_saved():
    compat = EventsCompatibility.parse_obj({"source_system_value": "ser", "source_system_name": "Ser"})
    assert (compat.source_system_value, compat.source_system_name) == ("ser", "Ser")


def test_untouched_and_older_blank_switches_behave_as_before():
    for compatibility in ({}, {f: None for f in SWITCHES}, {f: True for f in SWITCHES}):
        config = SyncEventsConfig.parse_obj({"compatibility": compatibility})
        settings = event_settings(SITE, config)
        assert settings.prepend_categories and settings.prepend_event_types and settings.prepend_titles
        assert settings.update_schema and settings.delete_unmatched
        assert compatibility_overrides(config, SITE) == []
