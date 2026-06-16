# Local development

## Run the tests

```bash
pytest
```

Tests use `pytest-asyncio` and `pytest-mock`; all external services (Gundi API, the ER client, PubSub,
Redis) are mocked via fixtures in `app/conftest.py` and `app/actions/tests/conftest.py`. Action tests live
in `app/actions/tests/`.

## Run the service locally

The runner is a FastAPI service and can be brought up with Docker Compose against Gundi stage services.
Full steps are in [`local/LOCAL_DEVELOPMENT.md`](https://github.com/PADAS/gundi-integration-earthranger/blob/main/local/LOCAL_DEVELOPMENT.md);
in short:

1. In `local/`, copy `.env.local.example` to `.env.local` and set `KEYCLOAK_CLIENT_SECRET` to a stage
   secret (ask the Gundi team).
2. Compile `requirements.txt` if you've changed any `*.in` files (otherwise the image may miss deps).
3. `docker compose up --build`.

The browsable API is then at <http://localhost:8080/docs>.

## Dependencies

Runtime/dev dependencies are compiled from the `*.in` files:

```bash
pip-compile --output-file=requirements.txt requirements-base.in requirements-dev.in requirements.in
```

Documentation dependencies are separate, in `requirements-docs.txt` (`mkdocs` + `mkdocs-material`).

## Working on these docs

```bash
pip install -r requirements-docs.txt
mkdocs serve            # live-reload preview at http://127.0.0.1:8000
mkdocs build --strict   # what CI runs; fails on broken links/nav
```

The docs site is published to GitHub Pages automatically on push to `main` (see
[`.github/workflows/docs.yml`](https://github.com/PADAS/gundi-integration-earthranger/blob/main/.github/workflows/docs.yml)).
Brainstorming specs under `docs/superpowers/` are excluded from the published site.
