# wrike_migration/steps/attachments.py
"""
Step 6: Fetch attachment URLs from Wrike and update Airtable attachment fields.

Processes both projects and tasks — skips whichever section doesn't have
an ``attachments`` config key.
Uses ``?withUrls=true`` to get download URLs inline with the attachment metadata.
"""

import time
from typing import Any, Dict, List, Optional

from core import field_ref, field_name
from core.wrike_client import WrikeClient
from core.airtable_client import AirtableClient
from core.lookups import LookupManager
from core.cache import save_json, load_json


def _map_attachments(attachments: List[Dict[str, Any]]) -> List[Dict[str, str]]:
    """Map Wrike attachments to Airtable attachment format.

    Tries ``url``, ``previewUrl``, ``playlistUrl`` in order.
    """
    result = []
    for att in attachments:
        url = att.get("url") or att.get("previewUrl") or att.get("playlistUrl")
        if not url:
            continue
        entry: Dict[str, str] = {"url": url}
        name = att.get("name")
        if name:
            entry["filename"] = name
        result.append(entry)
    return result


def _process_section(
    section_name: str,
    section_config: Dict[str, Any],
    table_config: Dict[str, Any],
    wrike: WrikeClient,
    airtable: AirtableClient,
    dry_run: bool,
    limit: Optional[int],
    cache_dir: str,
    use_cache: bool = False,
) -> Dict[str, int]:
    """Fetch attachments for one section (projects or tasks) and upsert."""
    table_id = table_config["table_id"]
    upsert_key_ref = field_ref(table_config["upsert_key"])

    # Try loading from cache first
    if use_cache:
        cached = load_json(cache_dir, f"attachments_{section_name}", "airtable_ready")
        if cached is not None:
            print(f"\n  [{section_name}] Loaded {len(cached)} records from cache")
            upsert_result = airtable.upsert(
                table_id,
                cached,
                key_fields=[upsert_key_ref],
                dry_run=dry_run,
            )
            return {"resolved": len(cached), **upsert_result}

    upsert_key_name = field_name(table_config["upsert_key"])

    att_count_target = section_config.get("field_mapping", {}).get("attachmentCount")
    att_count_name = field_name(att_count_target) if att_count_target else "attachmentCount (wrike)"

    attachment_ref = field_ref(section_config["attachments"])

    # Wrike API path differs by entity type
    wrike_path = "folders" if section_name == "projects" else "tasks"

    print(f"\n  [{section_name}] Fetching records with attachments from Airtable...")
    records = airtable.fetch_all(
        table_id,
        fields=[upsert_key_name, att_count_name],
    )

    items_with_attachments = []
    for rec in records:
        fields = rec.get("fields", {})
        wrike_id = fields.get(upsert_key_name)
        count = fields.get(att_count_name, 0)
        if wrike_id and count and int(count) > 0:
            items_with_attachments.append(wrike_id)

    print(f"  [{section_name}] Found {len(items_with_attachments)} with attachments")

    if limit:
        items_with_attachments = items_with_attachments[:limit]

    updates: List[Dict[str, Any]] = []
    total_resolved = 0
    total_skipped = 0

    for idx, wrike_id in enumerate(items_with_attachments, 1):
        try:
            data = wrike.get(
                f"{wrike_path}/{wrike_id}/attachments",
                params={"withUrls": "true"},
            )
            attachments = data.get("data", [])

            if attachments:
                att_objects = _map_attachments(attachments)
                total_resolved += len(att_objects)
                total_skipped += len(attachments) - len(att_objects)

                if att_objects:
                    updates.append({
                        upsert_key_ref: wrike_id,
                        attachment_ref: att_objects,
                    })

        except Exception as e:
            print(f"    Error fetching attachments for {wrike_id}: {e}")

        if idx % 50 == 0:
            print(f"    [{section_name}] Progress: {idx}/{len(items_with_attachments)} ({total_resolved} URLs resolved)")

        time.sleep(0.1)

    print(f"  [{section_name}] Prepared {len(updates)} records with {total_resolved} attachments ({total_skipped} skipped)")

    save_json(cache_dir, f"attachments_{section_name}", "airtable_ready", updates)

    upsert_result = airtable.upsert(
        table_id,
        updates,
        key_fields=[upsert_key_ref],
        dry_run=dry_run,
    )

    return {"resolved": len(updates), **upsert_result}


def run(
    config: Dict[str, Any],
    wrike: WrikeClient,
    airtable: AirtableClient,
    lookups: LookupManager,
    dry_run: bool = False,
    limit: Optional[int] = None,
    cache_dir: str = "",
    use_cache: bool = False,
    **kwargs,
) -> Dict[str, Any]:
    """Fetch attachments from Wrike and update Airtable."""
    totals: Dict[str, int] = {"resolved": 0, "created": 0, "updated": 0}

    for section_name in ("projects", "tasks"):
        section_config = config.get(section_name, {})
        if "attachments" not in section_config:
            print(f"  No attachments config for {section_name}, skipping.")
            continue

        table_key = section_name if section_name in config["airtable"]["tables"] else None
        if not table_key:
            continue
        table_config = config["airtable"]["tables"][table_key]

        result = _process_section(
            section_name, section_config, table_config,
            wrike, airtable, dry_run, limit, cache_dir, use_cache,
        )

        for k in totals:
            totals[k] += result.get(k, 0)

    return totals
