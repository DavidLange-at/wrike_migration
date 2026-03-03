"""
Core utilities for the Wrike migration pipeline.
"""

from typing import Any, List, Union


def field_ref(config_value: Any) -> str:
    """Extract field reference for Airtable API writes.

    Prefers field ID (fldXXX), falls back to field name.
    Handles: plain string (legacy), {"name": "...", "id": "fldXXX"}.
    """
    if isinstance(config_value, dict):
        fid = config_value.get("id")
        if fid and fid != "FILL_IN":
            return fid
        return config_value["name"]
    return str(config_value)


def field_name(config_value: Any) -> str:
    """Extract human-readable field name for display and Airtable reads."""
    if isinstance(config_value, dict):
        return config_value["name"]
    return str(config_value)


def field_refs(config_value: Any) -> List[str]:
    """Extract field references from a single or list config value.

    Used for upsert_key which can be a single field or a list of up to 3.
    """
    if isinstance(config_value, list):
        return [field_ref(item) for item in config_value]
    return [field_ref(config_value)]


def field_names(config_value: Any) -> List[str]:
    """Extract field names from a single or list config value."""
    if isinstance(config_value, list):
        return [field_name(item) for item in config_value]
    return [field_name(config_value)]
