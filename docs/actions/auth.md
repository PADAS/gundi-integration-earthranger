# `auth`

Validates the configured EarthRanger credentials. This is the action Gundi runs to verify a connection.

`action_auth` — `app/actions/handlers.py`

## Behavior

Authentication supports two methods, selected by `authentication_type`:

- **Token** — calls the ER client's `get_me()` and confirms the returned user is active.
- **Username / password** — performs an OAuth `login()` against the ER site.

It returns a small result the portal interprets as valid / invalid:

```json
{ "valid_credentials": true }
```

```json
{ "valid_credentials": false, "error": "Invalid credentials" }
```

## Error cases

The action returns `valid_credentials: false` with a descriptive `error` for each failure mode:

| Condition | `error` |
|-----------|---------|
| Missing/invalid ER site URL | `Site URL is empty or invalid: '<url>'` |
| Token method, no token | `Please provide a token.` |
| Username/password method, missing either | `Please provide both a username and a password.` |
| Unknown `authentication_type` | `Please select an valid authentication method.` |
| ER rejects the credentials | `Invalid credentials` |
| Other ER client error | the ER client's message |
| Transport/HTTP error | `HTTP error: <detail>` |

## Configuration — `AuthenticateConfig`

| Field | Type | Default | Notes |
|-------|------|---------|-------|
| `authentication_type` | enum (`token`, `username_password`) | `token` | Selects which credential fields apply. |
| `token` | secret string | — | Shown/required when type is `token`. |
| `username` | string | — | Shown/required when type is `username_password`. |
| `password` | secret string | — | Shown/required when type is `username_password`. |

The config form uses a conditional JSON schema: choosing `token` shows only the token field; choosing
`username_password` shows username and password. The ER **base URL** comes from the integration itself,
not this config.
