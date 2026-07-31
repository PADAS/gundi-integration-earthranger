# Issue: Raw enum values shown in the portal's Authentication Type dropdown

## Problem

When configuring EarthRanger as a destination in the Gundi Portal, the
**Authentication Type** dropdown in the `auth` action form shows the raw enum
values:

- `token`
- `username_password`

This is not polished for a configuration screen that end users see. The field
label also falls back to the raw class name (`ERAuthenticationType`) instead of
a human-friendly title.

## Root cause

`ERAuthenticationType` in `app/actions/configurations.py` is a plain
`str`-based `Enum`. Pydantic emits it in the JSON Schema as a bare `enum` in
`definitions`, referenced from the `authentication_type` property via
`allOf`/`$ref`:

```json
"authentication_type": {
  "allOf": [{"$ref": "#/definitions/ERAuthenticationType"}],
  "default": "token",
  "description": "Type of authentication to use."
},
"definitions": {
  "ERAuthenticationType": {
    "enum": ["token", "username_password"],
    "type": "string",
    "title": "ERAuthenticationType"
  }
}
```

JSON Schema `enum` carries no display labels, so the portal (rjsf) renders the
values verbatim.

Editing the JSON Schema manually in the Gundi Django admin is only a temporary
workaround: `python -m app.register` **upserts** action schemas, so the next
re-registration of this integration type overwrites any manual edit.

## Potential solution

In `AuthenticateConfig.Config.schema_extra` (which already customizes this
schema to build the `if`/`then`/`else` token-vs-credentials switch), replace
the `authentication_type` property with an inline `oneOf` of `const`/`title`
pairs:

```python
schema["properties"]["authentication_type"] = {
    "type": "string",
    "title": "Authentication Type",
    "description": "Type of authentication to use.",
    "default": "token",
    "oneOf": [
        {"const": "token", "title": "Token"},
        {"const": "username_password", "title": "Username & Password"},
    ],
}
schema.get("definitions", {}).pop("ERAuthenticationType", None)
```

Notes:

- Stored values are unchanged (`"token"` / `"username_password"`), so existing
  configs keep validating and the `if`/`then`/`else` condition still matches —
  no data migration needed.
- `oneOf` + `const` + `title` are standard JSON Schema keywords accepted by
  ajv in strict mode (unlike the deprecated rjsf `enumNames` extension, which
  ajv strict rejects).
- rjsf renders a `oneOf` of consts as a select showing each option's `title`.
- After merging, re-register the integration type
  (`python -m app.register --slug earth_ranger`) so the platform picks up the
  new schema.
