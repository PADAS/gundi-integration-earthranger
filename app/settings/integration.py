# Add your integration-specific settings here
from .base import env

# Display name used when this action runner registers itself in Gundi.
# The product name is one word — the slug-derived fallback in
# self-registration would render "Earth Ranger".
INTEGRATION_TYPE_NAME = env.str("INTEGRATION_TYPE_NAME", "EarthRanger")
