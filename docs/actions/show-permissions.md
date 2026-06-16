# `show_permissions`

A diagnostic action. It introspects what the configured EarthRanger account can see and returns the slugs
and UUIDs you need to configure the pull actions. It changes nothing.

`action_show_permissions` — `app/actions/handlers.py`

## What it returns

The result renders in the portal as a card with these sections:

| Section | Contents |
|---------|----------|
| **User Details** | Full name, username, superuser flag (from ER `/users/me`). |
| **Global Permissions** | The account's permissions for event categories, event types, events, messages, and observations. |
| **Event Categories** | Each category, the event types under it, and the account's permissions on it. |
| **Event Type Slugs** | Flat list — paste into `pull_events`' `event_types`. |
| **Event Category Slugs** | Flat list — paste into `pull_events`' `event_categories`. |
| **Subject Groups** | Each group and the subjects within it (and, optionally, its sub-groups). |
| **Subject Group UUIDs** | Every group UUID, including nested ones — paste into `pull_observations`' `subject_group_ids`. |

The subject-group tree is walked recursively, so nested sub-group UUIDs (which aren't reachable from a flat
listing) are surfaced too — those are exactly the IDs `pull_observations` needs to filter by a sub-group.

## How operators use it

1. Run it after [`auth`](auth.md) succeeds.
2. Copy **Event Type Slugs** / **Event Category Slugs** into [`pull_events`](pull-events.md).
3. Copy **Subject Group UUIDs** into [`pull_observations`](pull-observations.md).
4. Use **User Details** / **Global Permissions** to diagnose "why am I not seeing X?" (usually a
   permissions gap on the account).

## Configuration — `ShowPermissionsConfig`

| Field | Type | Default | Notes |
|-------|------|---------|-------|
| `include_subjects_from_subgroups_in_parent` | bool | `True` | When listing a group's subjects, also include subjects from its sub-groups. |
