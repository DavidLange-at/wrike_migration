"""Config-driven field mapping engine.

Transforms raw Wrike API responses into Airtable-ready records using JSON
configuration that defines source paths, target fields, value maps, named
transforms, and linked-field resolution.
"""

from __future__ import annotations

import json
import logging
from typing import Any, Optional

from markdownify import markdownify as md

from core.lookups import LookupManager, match_status_fuzzy

logger = logging.getLogger(__name__)

# Sentinel value used to mark Contacts custom fields so that
# apply_field_mapping can route them through people-type linked resolution.
_CONTACTS_MARKER = "__contacts__"


# ---------------------------------------------------------------------------
# Named transforms
# ---------------------------------------------------------------------------

def _transform_html_to_markdown(value: Any) -> Optional[str]:
    """Convert HTML to markdown, preserving hyperlinks."""
    if value is None:
        return None
    text = md(str(value)).strip()
    return text or None


def _transform_boolean(value: Any) -> Optional[bool]:
    """Convert common truthy/falsy string representations to Python bool."""
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    normalized = str(value).strip().lower()
    if normalized in ("yes", "true", "1"):
        return True
    if normalized in ("no", "false", "0"):
        return False
    return None


def _transform_stringify_array(value: Any) -> Optional[str]:
    """Convert a list to a comma-separated string."""
    if value is None:
        return None
    if isinstance(value, list):
        return ", ".join(str(v) for v in value)
    return str(value)


def _transform_attachment_urls(value: Any) -> Optional[list[dict[str, str]]]:
    """Normalize attachment values to ``[{"url": "..."}]`` format."""
    if value is None:
        return None
    if not isinstance(value, list):
        value = [value]

    result: list[dict[str, str]] = []
    for item in value:
        if isinstance(item, dict):
            url = item.get("url") or item.get("URL") or item.get("contentUrl")
            if url:
                result.append({"url": str(url)})
        elif isinstance(item, str) and item.strip():
            result.append({"url": item.strip()})
    return result or None


_TRANSFORMS: dict[str, Any] = {
    "html_to_markdown": _transform_html_to_markdown,
    "boolean": _transform_boolean,
    "stringify_array": _transform_stringify_array,
    "attachment_urls": _transform_attachment_urls,
}


# ---------------------------------------------------------------------------
# Custom field resolution
# ---------------------------------------------------------------------------

def _resolve_custom_fields(
    raw_record: dict[str, Any],
    custom_field_names: dict[str, str],
    custom_field_types: dict[str, str],
) -> dict[str, Any]:
    """Flatten the raw ``customFields`` array into a name-keyed dict.

    Raw Wrike API returns::

        "customFields": [
            {"id": "IEAAB...", "value": "Yes"},
            {"id": "IEAAC...", "value": "KUANVWAJ"},
            ...
        ]

    This function resolves each field ID to its human-readable name via
    *custom_field_names* and returns a dict keyed by that name.

    For ``"Multiple"`` type fields the value (a JSON-encoded list string)
    is joined into a comma-separated string.

    For ``"Contacts"`` type fields the value is wrapped in a dict with the
    sentinel key ``_CONTACTS_MARKER`` so that the main mapping loop can
    detect it and route the IDs through people-type linked resolution.
    """
    custom_fields_raw = raw_record.get("customFields") or []
    resolved: dict[str, Any] = {}

    for entry in custom_fields_raw:
        field_id = entry.get("id")
        if not field_id:
            continue

        name = custom_field_names.get(field_id)
        if not name:
            logger.debug("Unknown custom field ID %s — skipping", field_id)
            continue

        field_type = custom_field_types.get(field_id, "")

        # Determine value: Wrike uses "value" for single values, sometimes
        # "values" for multi-select, but normally encodes multi-select as a
        # JSON list string inside "value".
        value = entry.get("value")
        values = entry.get("values")
        raw_value = values if values not in (None, []) else value

        if raw_value is None:
            continue

        # "Multiple" type fields store a JSON-encoded list string.
        if field_type == "Multiple":
            if isinstance(raw_value, str):
                try:
                    parsed = json.loads(raw_value)
                    if isinstance(parsed, list):
                        raw_value = ", ".join(str(v) for v in parsed)
                except (json.JSONDecodeError, TypeError):
                    pass
            elif isinstance(raw_value, list):
                raw_value = ", ".join(str(v) for v in raw_value)

        # "Contacts" type: the value is one or more Wrike user IDs.
        if field_type == "Contacts":
            # Normalize to a list of IDs.
            if isinstance(raw_value, str):
                ids = [v.strip() for v in raw_value.split(",") if v.strip()]
            elif isinstance(raw_value, list):
                ids = [str(v) for v in raw_value]
            else:
                ids = [str(raw_value)]
            # Mark with sentinel so apply_field_mapping routes through people lookup.
            raw_value = {_CONTACTS_MARKER: ids}

        resolved[name] = raw_value

    return resolved


# ---------------------------------------------------------------------------
# Source path extraction
# ---------------------------------------------------------------------------

def _get_nested(data: dict[str, Any], dotpath: str) -> Any:
    """Walk a dot-separated path into a nested dict.

    >>> _get_nested({"project": {"startDate": "2024-01-01"}}, "project.startDate")
    '2024-01-01'
    """
    parts = dotpath.split(".")
    current: Any = data
    for part in parts:
        if not isinstance(current, dict):
            return None
        current = current.get(part)
        if current is None:
            return None
    return current


def _extract_value(
    raw_record: dict[str, Any],
    source_path: str,
    custom_fields_resolved: dict[str, Any],
) -> Any:
    """Extract a value from *raw_record* according to *source_path*.

    Supported path forms:

    - ``"title"`` — top-level key.
    - ``"project.startDate"`` — nested dot-path.
    - ``"customField.[CS] Plus-Up"`` — custom field by name.
    - ``"parentIds|superParentIds"`` — alternative paths (try first, fall
      back to second).
    """
    # Alternative paths: "parentIds|superParentIds"
    if "|" in source_path:
        for alt in source_path.split("|"):
            val = _extract_value(raw_record, alt.strip(), custom_fields_resolved)
            if val is not None:
                return val
        return None

    # Custom field path: "customField.<name>"
    if source_path.startswith("customField."):
        cf_name = source_path[len("customField."):]
        return custom_fields_resolved.get(cf_name)

    # Dot-path or top-level key.
    return _get_nested(raw_record, source_path)


# ---------------------------------------------------------------------------
# Target parsing and field ID map
# ---------------------------------------------------------------------------

def _parse_targets(targets: Any) -> list[str]:
    """Normalize field_mapping targets into a list of field names.

    Handles:
      - "Field Name"                              -> ["Field Name"]
      - {"name": "Field Name", "id": "fldXXX"}   -> ["Field Name"]
      - ["Field A", {"name": "Field B", ...}]     -> ["Field A", "Field B"]
    """
    if isinstance(targets, str):
        return [targets]
    if isinstance(targets, dict):
        return [targets["name"]]
    if isinstance(targets, list):
        return [
            t["name"] if isinstance(t, dict) else str(t)
            for t in targets
        ]
    return [str(targets)]


def _build_field_id_map(field_mapping: dict[str, Any]) -> dict[str, str]:
    """Build a {field_name: field_id} map from field_mapping config.

    Only includes entries where the field ID is present and not a
    placeholder.  Returns an empty dict if no IDs are configured,
    which means output keys stay as field names.
    """
    id_map: dict[str, str] = {}

    for _source, targets in field_mapping.items():
        entries: list[Any]
        if isinstance(targets, list):
            entries = targets
        else:
            entries = [targets]

        for entry in entries:
            if isinstance(entry, dict):
                name = entry.get("name")
                fid = entry.get("id")
                if name and fid and fid != "FILL_IN":
                    id_map[name] = fid

    return id_map


# ---------------------------------------------------------------------------
# Linked field resolution
# ---------------------------------------------------------------------------

def _normalize_ids(value: Any) -> list[str]:
    """Coerce *value* into a flat list of ID strings.

    Accepts a list, a single string (possibly comma-separated), or a scalar.
    """
    if value is None:
        return []
    if isinstance(value, list):
        ids: list[str] = []
        for v in value:
            ids.extend(_normalize_ids(v))
        return ids
    text = str(value).strip()
    if not text:
        return []
    if "," in text:
        return [part.strip() for part in text.split(",") if part.strip()]
    return [text]


def _resolve_linked_field(
    value: Any,
    linked_config: dict[str, Any],
    lookups: LookupManager,
    status_names: Optional[dict[str, str]] = None,
) -> Optional[list[str]]:
    """Resolve Wrike IDs to Airtable Record IDs via a lookup table.

    Returns a list of record IDs, or ``None`` if nothing resolves.
    """
    link_type = linked_config.get("type", "")
    match_mode = linked_config.get("match", "exact")

    # Fuzzy status matching is a special case: input is a string, not IDs.
    if link_type == "status" and match_mode == "fuzzy":
        status_lookup = lookups.get("status")
        # Resolve opaque Wrike statusId to human-readable name first.
        status_str = str(value) if value is not None else ""
        if status_names:
            status_str = status_names.get(status_str, status_str)
        record_id = match_status_fuzzy(status_str, status_lookup)
        return [record_id] if record_id else None

    # For people / projects / other lookup types: resolve IDs.
    ids = _normalize_ids(value)
    if not ids:
        return None

    lookup_table = lookups.get(link_type)
    resolved: list[str] = []
    for wrike_id in ids:
        record_id = lookup_table.get(wrike_id)
        if record_id:
            resolved.append(record_id)
        else:
            logger.debug(
                "Linked field lookup miss: type=%s, wrike_id=%s",
                link_type,
                wrike_id,
            )

    return resolved or None


# ---------------------------------------------------------------------------
# Main mapping function
# ---------------------------------------------------------------------------

def apply_field_mapping(
    raw_record: dict[str, Any],
    section_config: dict[str, Any],
    lookups: LookupManager,
    custom_field_names: dict[str, str],
    custom_field_types: dict[str, str],
    status_names: Optional[dict[str, str]] = None,
) -> dict[str, Any]:
    """Map a raw Wrike API record to Airtable fields using *section_config*.

    Parameters
    ----------
    raw_record:
        A single record dict straight from the Wrike API (or a pre-flattened
        export record).
    section_config:
        The JSON config for this section/table.  Expected keys:
        ``field_mapping``, and optionally ``linked_fields``, ``transforms``,
        ``value_maps``.
    lookups:
        A :class:`LookupManager` instance for resolving linked fields.
    custom_field_names:
        Mapping of Wrike custom field ID to human-readable name.
    custom_field_types:
        Mapping of Wrike custom field ID to type string (e.g.
        ``"Contacts"``, ``"Multiple"``).

    Returns
    -------
    dict[str, Any]
        A dict keyed by Airtable field names, ready for upsert.
    """
    field_mapping: dict[str, Any] = section_config.get("field_mapping", {})
    linked_fields: dict[str, dict[str, Any]] = section_config.get("linked_fields", {})
    transforms: dict[str, str] = section_config.get("transforms", {})
    value_maps: dict[str, dict[str, str]] = section_config.get("value_maps", {})

    # Pre-resolve custom fields once per record.
    cf_resolved = _resolve_custom_fields(raw_record, custom_field_names, custom_field_types)

    output: dict[str, Any] = {}

    for source_path, targets in field_mapping.items():
        target_list = _parse_targets(targets)

        raw_value = _extract_value(raw_record, source_path, cf_resolved)

        # Detect Contacts-marker values from custom field resolution.
        is_contacts = (
            isinstance(raw_value, dict) and _CONTACTS_MARKER in raw_value
        )
        contacts_ids: list[str] = []
        if is_contacts:
            contacts_ids = raw_value[_CONTACTS_MARKER]
            # For processing, use the IDs list as the raw value.
            raw_value = contacts_ids

        if raw_value is None:
            continue

        for target_field in target_list:
            value = raw_value

            # 1. Apply value_maps (if target has one).
            if target_field in value_maps:
                vmap = value_maps[target_field]
                if isinstance(value, list):
                    value = [vmap.get(str(v), v) for v in value]
                else:
                    value = vmap.get(str(value), value)

            # 2. Apply named transforms (if target has one).
            if target_field in transforms:
                transform_name = transforms[target_field]
                transform_fn = _TRANSFORMS.get(transform_name)
                if transform_fn:
                    value = transform_fn(value)
                else:
                    logger.warning(
                        "Unknown transform '%s' for target '%s'",
                        transform_name,
                        target_field,
                    )

            if value is None:
                continue

            # 3. Resolve linked fields.
            if target_field in linked_fields:
                value = _resolve_linked_field(
                    value, linked_fields[target_field], lookups, status_names
                )
            elif is_contacts:
                # Contacts custom fields always route through people lookup
                # even if not explicitly declared in linked_fields.
                value = _resolve_linked_field(
                    value, {"type": "people"}, lookups
                )
            else:
                # 4. For non-linked array values where the target name
                #    contains "(wrike)" or "IDs", stringify as comma-separated.
                if isinstance(value, list) and (
                    "(wrike)" in target_field or "IDs" in target_field
                ):
                    value = ", ".join(str(v) for v in value)

            if value is None:
                continue

            output[target_field] = value

    # Translate field names to field IDs where configured.
    field_id_map = _build_field_id_map(field_mapping)
    if field_id_map:
        output = {
            field_id_map.get(key, key): value
            for key, value in output.items()
        }

    return output
