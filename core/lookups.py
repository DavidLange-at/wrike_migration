"""Auto-fetch lookup tables from Airtable at runtime.

Replaces manual CSV exports with cached, on-demand lookups that map
Wrike identifiers to Airtable Record IDs (or other field values).
"""

from __future__ import annotations

import json
import logging
import os
import re
import unicodedata
from typing import Any

from core.airtable_client import AirtableClient

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Lookup configuration types
# ---------------------------------------------------------------------------
# Each entry in the config dict looks like:
#   "people": {"table_id": "tblXXX", "key_field": "Wrike ID", "value_field": "Record ID"}

LookupConfig = dict[str, dict[str, str]]


# ---------------------------------------------------------------------------
# LookupManager
# ---------------------------------------------------------------------------

class LookupManager:
    """Fetches and caches key->value lookup dicts from Airtable tables.

    Parameters
    ----------
    client:
        An ``AirtableClient`` instance used to read Airtable data.
    lookup_config:
        Mapping of *lookup_name* -> table/field specification.  Each spec
        must contain ``table_id``, ``key_field``, and ``value_field``.
        When ``value_field`` is ``"Record ID"``, the Airtable record's own
        ``id`` (e.g. ``recXXX``) is used as the mapped value.
    """

    def __init__(
        self,
        client: AirtableClient,
        lookup_config: LookupConfig,
        cache_dir: str | None = None,
        use_cache: bool = False,
    ) -> None:
        self._client = client
        self._config = lookup_config
        self._cache: dict[str, dict[str, str]] = {}
        self._cache_dir = cache_dir
        self._use_cache = use_cache

    # -- public API ---------------------------------------------------------

    def get(self, lookup_name: str) -> dict[str, str]:
        """Return the lookup dict for *lookup_name*, fetching on first access."""
        if lookup_name not in self._config:
            raise KeyError(
                f"Unknown lookup '{lookup_name}'. "
                f"Available: {', '.join(sorted(self._config))}"
            )

        if lookup_name not in self._cache:
            # Try disk cache first when --use-cache is set
            if self._use_cache and self._cache_dir:
                disk_data = self._load_from_disk(lookup_name)
                if disk_data is not None:
                    self._cache[lookup_name] = disk_data
                    return self._cache[lookup_name]

            self._cache[lookup_name] = self._fetch(lookup_name)

            # Save to disk for future use
            if self._cache_dir:
                self._save_to_disk(lookup_name, self._cache[lookup_name])

        return self._cache[lookup_name]

    def refresh(self, lookup_name: str) -> None:
        """Clear the cache for *lookup_name* so the next ``get()`` re-fetches."""
        self._cache.pop(lookup_name, None)
        # Force re-fetch now and update disk cache
        self._cache[lookup_name] = self._fetch(lookup_name)
        if self._cache_dir:
            self._save_to_disk(lookup_name, self._cache[lookup_name])
        logger.debug("Refreshed lookup '%s'", lookup_name)

    def refresh_all(self) -> None:
        """Clear all cached lookups."""
        self._cache.clear()
        logger.debug("Cleared all lookup caches")

    # -- internals ----------------------------------------------------------

    def _fetch(self, lookup_name: str) -> dict[str, str]:
        """Fetch all records for *lookup_name* and build the key->value map."""
        spec = self._config[lookup_name]
        table_id: str = spec["table_id"]
        key_field: str = spec["key_field"]
        value_field: str = spec["value_field"]
        use_record_id = value_field == "Record ID"

        # Only request the fields we actually need from Airtable.
        fields = [key_field] if use_record_id else [key_field, value_field]

        records: list[dict[str, Any]] = self._client.fetch_all(
            table_id, fields=fields
        )

        lookup: dict[str, str] = {}
        for record in records:
            rec_fields = record.get("fields", {})
            key = rec_fields.get(key_field)
            if key is None:
                continue

            key_str = str(key)

            if use_record_id:
                value = record["id"]
            else:
                raw_value = rec_fields.get(value_field)
                if raw_value is None:
                    continue
                value = str(raw_value)

            lookup[key_str] = value

        logger.info(
            "Loaded lookup '%s': %d entries from table %s",
            lookup_name,
            len(lookup),
            table_id,
        )
        return lookup

    def _disk_path(self, lookup_name: str) -> str:
        """Return the disk cache path for a lookup."""
        return os.path.join(self._cache_dir, f"lookups_{lookup_name}.json")

    def _save_to_disk(self, lookup_name: str, data: dict[str, str]) -> None:
        """Save a lookup dict to disk."""
        os.makedirs(self._cache_dir, exist_ok=True)
        path = self._disk_path(lookup_name)
        with open(path, "w") as f:
            json.dump(data, f, indent=2)
        logger.info("Saved lookup '%s' to %s (%d entries)", lookup_name, os.path.basename(path), len(data))

    def _load_from_disk(self, lookup_name: str) -> dict[str, str] | None:
        """Load a lookup dict from disk. Returns None if not found."""
        path = self._disk_path(lookup_name)
        if not os.path.exists(path):
            print(f"    No cached lookup for '{lookup_name}', fetching from API...")
            return None
        with open(path, "r") as f:
            data = json.load(f)
        print(f"    Loaded lookup '{lookup_name}' from cache ({len(data)} entries)")
        return data


# ---------------------------------------------------------------------------
# Fuzzy status matching
# ---------------------------------------------------------------------------

# Regex that strips characters in common emoji Unicode blocks.
_EMOJI_RE = re.compile(
    "["
    "\U0000200d"          # zero-width joiner
    "\U0000200e"          # left-to-right mark (sometimes near emoji)
    "\U00002600-\U000027BF"  # misc symbols / dingbats
    "\U0000FE00-\U0000FE0F"  # variation selectors
    "\U0001F000-\U0001FAFF"  # main emoji blocks (mahjong through symbols)
    "\U000E0020-\U000E007F"  # tags
    "]+",
    flags=re.UNICODE,
)


def _normalize(text: str) -> str:
    """Lowercase, strip emoji, collapse whitespace."""
    text = _EMOJI_RE.sub("", text)
    # Also drop any remaining characters in the Symbol categories (So, Sk, …)
    # that the regex may have missed (e.g. keycap sequences).
    text = "".join(
        ch for ch in text
        if not unicodedata.category(ch).startswith("So")
    )
    return " ".join(text.lower().split())


def _has_emoji(text: str) -> bool:
    """Return True if *text* contains emoji characters."""
    return bool(_EMOJI_RE.search(text)) or any(
        unicodedata.category(ch).startswith("So") for ch in text
    )


def match_status_fuzzy(
    status: str,
    status_lookup: dict[str, str],
) -> str | None:
    """Match a Wrike status string to an Airtable Record ID.

    Matching strategy (applied in order):
    1. **Exact** -- normalized *status* equals a normalized lookup key.
    2. **Substring** -- normalized *status* is contained in a normalized key,
       or vice-versa.

    When multiple keys match at the same level, the one with an emoji prefix
    is preferred (these are the canonical entries in the status table).

    Returns the Airtable Record ID on match, or ``None``.
    """
    if not status or not status_lookup:
        return None

    norm_status = _normalize(status)
    if not norm_status:
        return None

    # Pre-compute normalized keys once.
    normalized_keys: list[tuple[str, str, str]] = [
        (key, _normalize(key), record_id)
        for key, record_id in status_lookup.items()
    ]

    # 1. Exact normalized match — prefer emoji-prefixed keys.
    exact_matches = [
        (key, record_id)
        for key, norm_key, record_id in normalized_keys
        if norm_status == norm_key
    ]
    if exact_matches:
        for key, record_id in exact_matches:
            if _has_emoji(key):
                return record_id
        return exact_matches[0][1]

    # 2. Substring match (either direction) — prefer emoji-prefixed keys.
    substr_matches = [
        (key, record_id)
        for key, norm_key, record_id in normalized_keys
        if norm_status in norm_key or norm_key in norm_status
    ]
    if substr_matches:
        for key, record_id in substr_matches:
            if _has_emoji(key):
                return record_id
        return substr_matches[0][1]

    return None
