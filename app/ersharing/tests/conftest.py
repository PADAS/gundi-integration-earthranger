import pytest

from app.ersharing import clients


@pytest.fixture(autouse=True)
def no_retry_pauses(monkeypatch):
    """Keep within-run retries but skip their pauses, so tests stay fast."""
    monkeypatch.setattr(clients, "RETRY_WAITS", (0, 0))


class FakeGundi:
    """Stands in for GundiClient where Share Subjects adds shared devices to Gundi's sources."""

    api_base_path = "https://gundi.test/v2"

    def __init__(self):
        self.posts = []
        self.status_code = 201

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    async def _post(self, url, data=None, **kwargs):
        self.posts.append((url, data))
        if isinstance(self.status_code, Exception):
            raise self.status_code
        return type("Response", (), {"status_code": self.status_code})()


@pytest.fixture(autouse=True)
def gundi(monkeypatch):
    from app.ersharing import gundi_sources
    fake = FakeGundi()
    monkeypatch.setattr(gundi_sources, "GundiClient", lambda: fake)
    return fake
