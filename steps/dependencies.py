# wrike_migration/steps/dependencies.py
"""
Step 5: Fetch task dependencies from Wrike and resolve to Airtable linked records.
"""

import json
import os
import time
from typing import Any, Dict, List, Optional, Set

from core import field_ref, field_name
from core.cache import save_json
from core.wrike_client import WrikeClient
from core.airtable_client import AirtableClient
from core.lookups import LookupManager


def _load_cache(cache_path: str) -> Dict[str, Any]:
    if os.path.exists(cache_path):
        with open(cache_path, "r") as f:
            return json.load(f)
    return {}


def _save_cache(cache_path: str, cache: Dict[str, Any]) -> None:
    with open(cache_path, "w") as f:
        json.dump(cache, f, indent=2)


def run(
    config: Dict[str, Any],
    wrike: WrikeClient,
    airtable: AirtableClient,
    lookups: LookupManager,
    dry_run: bool = False,
    limit: Optional[int] = None,
    data_dir: str = "data",
    cache_dir: str = "",
    **kwargs,
) -> Dict[str, Any]:
    """Fetch dependency relationships and update Airtable."""
    tasks_config = config["tasks"]
    dep_config = tasks_config.get("dependencies")
    if not dep_config:
        print("  No dependencies config for tasks, skipping.")
        return {}

    table_config = config["airtable"]["tables"]["tasks"]
    table_id = table_config["table_id"]
    upsert_key_ref = field_ref(table_config["upsert_key"])
    upsert_key_name = field_name(table_config["upsert_key"])

    dep_ids_name = field_name(dep_config["wrike_dependency_ids"])
    predecessor_ref = field_ref(dep_config["predecessor_field"])
    successor_ref = field_ref(dep_config["successor_field"])

    # Fetch all tasks from Airtable to get dependency IDs and record ID mapping
    print("  Fetching tasks from Airtable for dependency resolution...")
    records = airtable.fetch_all(
        table_id,
        fields=[upsert_key_name, dep_ids_name],
    )

    # Build mappings
    wrike_to_record: Dict[str, str] = {}
    all_dep_ids: Set[str] = set()
    tasks_with_deps: List[Dict[str, Any]] = []

    for rec in records:
        fields = rec.get("fields", {})
        wrike_id = fields.get(upsert_key_name)
        if wrike_id:
            wrike_to_record[wrike_id] = rec["id"]

        dep_ids_raw = fields.get(dep_ids_name, "")
        if dep_ids_raw:
            dep_ids = [d.strip() for d in str(dep_ids_raw).split(",") if d.strip()]
            if dep_ids:
                all_dep_ids.update(dep_ids)
                tasks_with_deps.append({
                    "wrike_id": wrike_id,
                    "dep_ids": dep_ids,
                })

    print(f"  {len(tasks_with_deps)} tasks with dependencies, {len(all_dep_ids)} unique dep IDs")

    if not all_dep_ids:
        print("  No dependencies to resolve.")
        return {}

    # Load/fetch dependency details with caching
    dep_cache_path = os.path.join(cache_dir, "dependencies_cache.json")
    cache = _load_cache(dep_cache_path)

    uncached = [did for did in all_dep_ids if did not in cache]
    print(f"  {len(all_dep_ids) - len(uncached)} cached, {len(uncached)} to fetch")

    if uncached and not dry_run:
        # Batch fetch in groups of 100
        for i in range(0, len(uncached), 100):
            batch = uncached[i:i + 100]
            ids_str = ",".join(batch)
            try:
                data = wrike.get(f"dependencies/{ids_str}")
                for dep in data.get("data", []):
                    cache[dep["id"]] = {
                        "predecessorId": dep["predecessorId"],
                        "successorId": dep["successorId"],
                    }
            except Exception as e:
                print(f"    Error fetching batch: {e}")

            if (i // 100 + 1) % 10 == 0:
                print(f"    Fetched {i + len(batch)}/{len(uncached)} dependency details")

        _save_cache(dep_cache_path, cache)
        print(f"  Cache saved ({len(cache)} entries)")

    # Build predecessor/successor relationships
    updates: List[Dict[str, Any]] = []
    for task in tasks_with_deps:
        wrike_id = task["wrike_id"]
        predecessors = []
        successors = []

        for dep_id in task["dep_ids"]:
            dep_data = cache.get(dep_id)
            if not dep_data:
                continue

            if dep_data["successorId"] == wrike_id:
                other = wrike_to_record.get(dep_data["predecessorId"])
                if other:
                    predecessors.append(other)
            elif dep_data["predecessorId"] == wrike_id:
                other = wrike_to_record.get(dep_data["successorId"])
                if other:
                    successors.append(other)

        if predecessors or successors:
            entry: Dict[str, Any] = {upsert_key_ref: wrike_id}
            if predecessors:
                entry[predecessor_ref] = predecessors
            if successors:
                entry[successor_ref] = successors
            updates.append(entry)

    if limit:
        updates = updates[:limit]

    print(f"  Resolved {len(updates)} tasks with dependency links")

    save_json(cache_dir, "dependencies", "airtable_ready", updates)

    upsert_result = airtable.upsert(
        table_id,
        updates,
        key_fields=[upsert_key_ref],
        dry_run=dry_run,
    )

    return {"resolved": len(updates), **upsert_result}
