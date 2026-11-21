# Add your integration-specific settings here
from .base import env

# Display name used when this action runner registers itself in Gundi.
# The product name is one word — the slug-derived fallback in
# self-registration would render "Earth Ranger".
INTEGRATION_TYPE_NAME = env.str("INTEGRATION_TYPE_NAME", "EarthRanger")

# Read timeout for EarthRanger calls made by Share Events / Share Subjects;
# erclient's default is 20 s, and the v1 sharing jobs used 60 s.
ER_DATA_TIMEOUT_SECONDS = env.float("ER_DATA_TIMEOUT_SECONDS", 60.0)
