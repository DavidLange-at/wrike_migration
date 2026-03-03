# wrike_migration/steps/project_links.py
"""
Step 2: Resolve parent/child project self-links after initial import.
"""

from typing import Any, Dict, List, Optional

from core import field_ref, field_name
from core.airtable_client import AirtableClient
from core.cache import save_json
from core.lookups import LookupManager


def run(
    config: Dict[str, Any],
    airtable: AirtableClient,
    lookups: LookupManager,
    dry_run: bool = False,
    limit: Optional[int] = None,
    cache_dir: str = "",
    **kwargs,
) -> Dict[str, Any]:
    """Resolve parent/child project self-links."""
    projects_config = config["projects"]
    self_links = projects_config.get("self_links")
    if not self_links:
        print("  No self_links config for projects, skipping.")
        return {}

    table_config = config["airtable"]["tables"]["projects"]
    table_id = table_config["table_id"]
    upsert_key_ref = field_ref(table_config["upsert_key"])
    upsert_key_name = field_name(table_config["upsert_key"])

    parent_ids_name = field_name(self_links["wrike_parent_ids"])
    child_ids_name = field_name(self_links["wrike_child_ids"])
    link_field_ref = field_ref(self_links["link_field"])

    # Fetch all projects from Airtable to build Wrike ID -> Record ID mapping
    print("  Fetching projects from Airtable for self-link resolution...")
    records = airtable.fetch_all(
        table_id,
        fields=[upsert_key_name, parent_ids_name, child_ids_name],
    )

    # Build Wrike ID -> Record ID map from fetched records
    wrike_to_record: Dict[str, str] = {}
    for rec in records:
        fields = rec.get("fields", {})
        wrike_id = fields.get(upsert_key_name)
        if wrike_id:
            wrike_to_record[wrike_id] = rec["id"]

    print(f"  Built mapping for {len(wrike_to_record)} projects")

    # Build update records using field refs (IDs) for output
    updates: List[Dict[str, Any]] = []
    for rec in records:
        fields = rec.get("fields", {})
        wrike_id = fields.get(upsert_key_name)
        if not wrike_id:
            continue

        # Parse child IDs (comma-separated Wrike IDs)
        child_ids_raw = fields.get(child_ids_name, "")
        if not child_ids_raw:
            continue

        child_wrike_ids = [cid.strip() for cid in str(child_ids_raw).split(",") if cid.strip()]
        resolved_children = [
            wrike_to_record[cid] for cid in child_wrike_ids if cid in wrike_to_record
        ]

        if resolved_children:
            updates.append({
                upsert_key_ref: wrike_id,
                link_field_ref: resolved_children,
            })

    if limit:
        updates = updates[:limit]

    print(f"  Resolved {len(updates)} project self-links")

    save_json(cache_dir, "project_links", "airtable_ready", updates)

    upsert_result = airtable.upsert(
        table_id,
        updates,
        key_fields=[upsert_key_ref],
        dry_run=dry_run,
    )

    return {"resolved": len(updates), **upsert_result}
