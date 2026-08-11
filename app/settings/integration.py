# Add your integration-specific settings here
from .base import env

# Display name used when this action runner registers itself in Gundi.
# The product name is one word — the slug-derived fallback in
# self-registration would render "Earth Ranger".
INTEGRATION_TYPE_NAME = env.str("INTEGRATION_TYPE_NAME", "EarthRanger")

# Phase 0 of the reference-data design (docs/superpowers/specs/
# 2026-07-31-reference-data-config-ui-design.md): reference actions are only
# registered in Gundi once the platform accepts the "reference" action type.
# Until then this stays off so self-registration never sends a type the API
# would reject.
REGISTER_REFERENCE_ACTIONS = env.bool("REGISTER_REFERENCE_ACTIONS", False)
