from app import settings


def test_integration_type_name_defaults_to_earthranger():
    # The product name is "EarthRanger" (one word). Without an explicit
    # setting, self-registration falls back to title-casing the slug,
    # which renders "Earth Ranger" in the Gundi portal.
    assert settings.INTEGRATION_TYPE_NAME == "EarthRanger"
