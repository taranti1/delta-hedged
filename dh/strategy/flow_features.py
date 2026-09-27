"""Feature identities are part of a fitted model's contract, not just its bucket names."""
from __future__ import annotations

from collections.abc import Mapping
from typing import Any

PRODUCTION_FEATURE_VERSION = "model_remaining_average_nearest_boundary_v1"
PROXY_FEATURE_VERSION = "spot_fixed_vol_nearest_boundary_v2"


def validate_feature_version(meta: Mapping[str, Any], expected: str = PRODUCTION_FEATURE_VERSION) -> None:
    actual = meta.get("feature_version")
    if actual != expected:
        raise ValueError(f"incompatible flow features: {actual or 'unversioned'}; expected {expected}. "
                         "Refit with the causal production model state before using these segments.")
