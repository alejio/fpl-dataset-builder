"""Validation package for FPL dataset builder."""

# Legacy schemas removed - now use raw_schemas.py and derived_schemas.py
from .validators import validate_dataframe

__all__ = [
    "validate_dataframe",
]
