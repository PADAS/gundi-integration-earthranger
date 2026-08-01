import importlib
import os
from unittest import mock

from app.settings import integration


def test_integration_type_name_defaults_to_earthranger():
    # The product name is "EarthRanger" (one word). Without an explicit
    # setting, self-registration falls back to title-casing the slug,
    # which renders "Earth Ranger" in the Gundi portal.
    # The module snapshots the env var at import time, so unset it and
    # reload to assert the default regardless of the ambient environment.
    try:
        with mock.patch.dict(os.environ):
            os.environ.pop("INTEGRATION_TYPE_NAME", None)
            reloaded = importlib.reload(integration)
            assert reloaded.INTEGRATION_TYPE_NAME == "EarthRanger"
    finally:
        # Re-read the real environment so other tests see the actual value.
        importlib.reload(integration)
