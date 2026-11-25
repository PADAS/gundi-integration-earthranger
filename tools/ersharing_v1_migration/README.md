# ER Sharing: move v1 shares to v2 routes

`migrate.py` turns Gundi v1 EarthRanger-to-EarthRanger shares (`er2er` events and
`er2er_subject_sharing` tracks) into Gundi v2 routes on the `earth_ranger`
integration type, following the route plan (`plan.json`).

It only needs Python 3.9+ and the standard library.

## What you need

- `plan.json` next to the script (or `--plan <path>`). It's made by the planning
  script `build.py` and holds no secrets.
- A portal token for the API in `~/.config/gundi/portal-token-prod` (or
  `--token-file`).
- For `--apply`: the `earth_ranger` type in v2 must have the `sync_events` and
  `sync_observations` actions. The script checks for them and refuses to write
  until they're there.

## Pick routes

These options can be combined:

```bash
python3 migrate.py --route R073 R021        # plan route ids
python3 migrate.py --v1-id <v1 config id>   # the route that holds this v1 config
python3 migrate.py --file routes.txt        # ids separated by spaces, commas or lines; # starts a comment
python3 migrate.py --wave 1                 # every wave 1 route
```

Routes outside wave 1 are refused unless you pass `--include-routes-not-in-wave-1`.

## Dry run (the default)

Without `--apply`, the script only reads. For each route it prints:

- the source integration it would create
- the `sync_events` and `sync_observations` settings
- each destination it would reuse or create
- the provider key v1 sends and the key v2 will send
- the route's field mappings

At the end it prints a summary with the reason for every refused route.

## Apply

```bash
python3 migrate.py --route R073 --apply
```

It prints the dry run first. Then it asks you to type `apply <count> to <api host>`
before it writes anything.

For each route it:

1. Creates any missing destinations: `earth_ranger` integrations at the destination
   site, owned by the route owner, with the v1 token.
2. Creates the source integration with its default route. The integration is off
   (`enabled: false`), and `run_on_schedule` is off on both sync actions.
3. Adds the destinations to the route. For tracks, it also sets each destination's
   provider key as a route field mapping:
   `field_mappings[<source id>]["obv"][<destination id>] = {"destination_field": "provider_key", "default": <key>}`.
4. Reads the route back and checks the destinations and provider keys.
5. Saves the ids in `mapping.json`, both by plan route and by v1 config id.

## Safety checks

- **Live v1 data:** tokens and settings come from the v1 API at run time, never
  from the plan. Tokens are never printed.
- **Plan drift:** a route is refused if its v1 configs are no longer enabled, or
  if v1 now sends to destinations the plan doesn't list. It's also refused if a
  destination no longer gets every kind of data in the route (events, tracks),
  because v2 sends all of the route's data to every destination.
- **Provider keys:** a route is refused if v1 now uses a different provider key,
  or if v2 would send a different key than v1. For example, v2 trims spaces from
  the key.
- **Data v2 can't hold:** a route is refused if one destination would need two
  tokens, or if a setting is stored as text instead of on/off.
- **Destination reuse:** a destination is reused only when it's an EarthRanger
  integration at the same site, with the same owner and the same token. Several
  routes in one run share a destination created earlier in that run.
- **Source reuse:** the source integration is matched only by the name this script
  gives it, so another connection of the owner is never reused. Reusing one would
  start sending this share's data to that connection's existing destinations.
- **Safe to re-run:** a route that's already complete is skipped. A route left
  half-done (integration created, no destinations yet) is finished on the next run.
  Either way, the integration's saved sync settings must still match v1 (apart from
  `run_on_schedule` and `start_from`). A half-done route is finished only while its
  integration is off and both sync actions have `run_on_schedule` off. The script
  reads the integration again right before it writes.
- **Exit code:** 0 when every route is ready or written, 2 when any route was
  refused, 1 when a write failed or wasn't confirmed.

## v1 calls

The script reads v1 only through these calls:

- `GET /api/v1.0/integrations/inbound/configurations?enabled=true`
- `GET /api/v1.0/integrations/outbound/configurations`
- `GET /integrations/devicegroups/<id>/destinations`

Never add `?inbound_id=` to the outbound list: with it, v1 creates a Device.
