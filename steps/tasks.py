# wrike_migration/steps/tasks.py
"""
Step 3: Fetch tasks (including recursive subtasks) from Wrike, transform, upsert.
"""

import time
from typing import Any, Dict, List, Optional, Set

from core import field_ref, field_refs
from core.wrike_client import WrikeClient
from core.airtable_client import AirtableClient
from core.lookups import LookupManager
from core.transforms import apply_field_mapping
from core.cache import save_json, load_json


TASK_FIELDS_QUERY = (
    "[metadata,hasAttachments,attachmentCount,description,briefDescription,"
    "customFields,superTaskIds,subTaskIds,dependencyIds,responsibleIds,"
    "authorIds,parentIds,sharedIds,recurrent,effortAllocation,billingType,customItemTypeId]"
)

# Multi-task endpoint supports fewer fields
SUBTASK_FIELDS_QUERY = (
    "[attachmentCount,recurrent,effortAllocation,billingType,customItemTypeId]"
)


def _fetch_tasks_for_folder(wrike: WrikeClient, folder_id: str) -> List[Dict[str, Any]]:
    """Fetch all tasks for a folder/project using pagination."""
    return wrike.get_paginated(
        f"folders/{folder_id}/tasks",
        params={"fields": TASK_FIELDS_QUERY},
        page_size=1000,
    )


def _fetch_subtasks_batch(wrike: WrikeClient, subtask_ids: List[str]) -> List[Dict[str, Any]]:
    """Fetch subtasks by ID in batches of up to 100."""
    all_subtasks: List[Dict[str, Any]] = []
    for i in range(0, len(subtask_ids), 100):
        batch = subtask_ids[i:i + 100]
        ids_param = ",".join(batch)
        data = wrike.get(f"tasks/{ids_param}", params={"fields": SUBTASK_FIELDS_QUERY})
        all_subtasks.extend(data.get("data", []))
    return all_subtasks


def _normalize_subtask_parents(subtasks: List[Dict[str, Any]]) -> None:
    """Replace parentIds with superParentIds on subtasks.

    Top-level tasks have correct project folder IDs in ``parentIds``.
    Subtasks often have a placeholder folder ID in ``parentIds`` but the
    real project ancestry in ``superParentIds``.  Normalizing here lets
    the transform engine's ``parentIds|superParentIds`` path work correctly.
    """
    for task in subtasks:
        super_parent_ids = task.get("superParentIds")
        if super_parent_ids:
            task["parentIds"] = super_parent_ids


def _fetch_subtasks_recursive(wrike: WrikeClient, initial_ids: List[str]) -> List[Dict[str, Any]]:
    """Recursively fetch all subtasks including nested levels."""
    all_subtasks: List[Dict[str, Any]] = []
    ids_to_fetch = list(initial_ids)
    fetched: Set[str] = set()

    while ids_to_fetch:
        ids_to_fetch = [sid for sid in ids_to_fetch if sid not in fetched]
        if not ids_to_fetch:
            break

        print(f"      Fetching {len(ids_to_fetch)} subtasks...")
        subtasks = _fetch_subtasks_batch(wrike, ids_to_fetch)
        fetched.update(ids_to_fetch)

        # Normalize: use superParentIds for project attribution
        _normalize_subtask_parents(subtasks)

        all_subtasks.extend(subtasks)

        # Collect next level
        ids_to_fetch = []
        for st in subtasks:
            ids_to_fetch.extend(st.get("subTaskIds") or [])

    return all_subtasks


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
    """Run the tasks step."""
    tasks_config = config["tasks"]
    table_config = config["airtable"]["tables"]["tasks"]

    # Fetch custom fields
    cf_names, cf_types = wrike.fetch_custom_fields()

    # Fetch status name mapping
    status_names = wrike.fetch_status_names()

    # Get project Wrike IDs from the projects lookup
    project_lookup = lookups.get("projects")
    project_wrike_ids = list(project_lookup.keys())
    print(f"  Found {len(project_wrike_ids)} projects to fetch tasks for")

    all_raw_unique: List[Dict[str, Any]] = []
    seen_wrike_ids: Set[str] = set()

    if use_cache:
        cached = load_json(cache_dir, "tasks", "wrike_raw")
        if cached is not None:
            all_raw_unique = cached
            seen_wrike_ids = {t.get("id") for t in cached}
            print(f"  Loaded {len(all_raw_unique)} tasks from cache")

    if not all_raw_unique:
        total_projects = len(project_wrike_ids)
        for idx, proj_id in enumerate(project_wrike_ids, 1):
            print(f"\n  Project {idx}/{total_projects}: {proj_id}")

            try:
                folder_tasks = _fetch_tasks_for_folder(wrike, proj_id)
            except Exception as e:
                print(f"    Error fetching tasks: {e}")
                continue

            if not folder_tasks:
                print(f"    No tasks found")
                continue

            print(f"    {len(folder_tasks)} top-level tasks")

            # Collect subtask IDs
            subtask_ids = []
            for task in folder_tasks:
                subtask_ids.extend(task.get("subTaskIds") or [])

            # Fetch subtasks recursively
            all_tasks_for_project = list(folder_tasks)
            if subtask_ids:
                print(f"    {len(subtask_ids)} subtask references")
                subtasks = _fetch_subtasks_recursive(wrike, subtask_ids)
                print(f"    {len(subtasks)} subtasks retrieved")
                all_tasks_for_project.extend(subtasks)

            # Deduplicate
            for task in all_tasks_for_project:
                wrike_id = task.get("id")
                if wrike_id not in seen_wrike_ids:
                    seen_wrike_ids.add(wrike_id)
                    all_raw_unique.append(task)

            # Early exit when limit is set and we have enough tasks
            if limit and len(all_raw_unique) >= limit:
                print(f"\n  Reached {len(all_raw_unique)} tasks (limit={limit}), stopping fetch early")
                break

            time.sleep(0.25)

    # Save raw data
    save_json(cache_dir, "tasks", "wrike_raw", all_raw_unique)

    # Transform
    all_mapped: List[Dict[str, Any]] = []
    for task in all_raw_unique:
        record = apply_field_mapping(
            task, tasks_config, lookups, cf_names, cf_types, status_names
        )
        if record:
            all_mapped.append(record)

    print(f"\n  Total: {len(all_mapped)} unique tasks transformed")

    if limit:
        all_mapped = all_mapped[:limit]
        print(f"  Limited to {limit} tasks")

    # Save transformed data
    save_json(cache_dir, "tasks", "airtable_ready", all_mapped)

    # Upsert into Airtable
    upsert_key = field_refs(table_config["upsert_key"])
    upsert_result = airtable.upsert(
        table_config["table_id"],
        all_mapped,
        key_fields=upsert_key,
        dry_run=dry_run,
    )

    # Refresh lookups for subsequent steps
    if not dry_run:
        lookups.refresh("projects")

    return {
        "wrike_fetched": len(all_raw_unique),
        "transformed": len(all_mapped),
        **upsert_result,
    }
