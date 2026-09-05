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
| Missing/invalid ER site URL | `Site URL is empty or invalid.` (the URL itself is never echoed: it may carry a token) |
| Token method, no token | `Please provide a token.` |
| Username/password method, missing either | `Please provide both a username and a password.` |
| Unknown `authentication_type` | `Please select an valid authentication method.` |
| ER rejects the credentials (401, 403, or the token endpoint's 400 `invalid_grant`) | `EarthRanger rejected the credentials` or `EarthRanger denied access with these credentials` |
| ER rate-limits the request | `EarthRanger rate-limited the request` |
| ER unreachable (DNS, connection, timeout, 502/503/504) | `EarthRanger could not be reached` |
| Any other ER failure | `EarthRanger returned an unexpected response` |

These are fixed texts: the ER client's own messages carry ER's response body, or the login request (which
includes the password), and neither may reach the result or the activity log. The same translation serves the
[reference actions](reference-actions.md).

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
