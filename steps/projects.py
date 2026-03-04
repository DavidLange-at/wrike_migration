# wrike_migration/steps/projects.py
"""
Step 1: Fetch projects from Wrike, transform, and upsert into Airtable.
"""

from typing import Any, Dict, List, Optional, Set

from core import field_ref, field_refs
from core.wrike_client import WrikeClient
from core.airtable_client import AirtableClient
from core.lookups import LookupManager
from core.transforms import apply_field_mapping
from core.cache import save_json, load_json


FIELDS_QUERY = (
    "[metadata,hasAttachments,attachmentCount,description,briefDescription,"
    "customFields,customColumnIds,superParentIds,space,contractType,customItemTypeId]"
)


def fetch_projects(wrike: WrikeClient, space_id: str) -> List[Dict[str, Any]]:
    """Fetch all projects for a Wrike space."""
    print(f"\n  Fetching projects for space {space_id}...")
    return wrike.get_paginated(
        f"spaces/{space_id}/folders",
        params={"descendants": "true", "project": "true", "fields": FIELDS_QUERY},
        page_size=200,
    )


def _resolve_parent_folders(
    raw_projects: List[Dict[str, Any]],
    wrike: WrikeClient,
) -> Dict[str, str]:
    """Resolve each project's ancestor folder (walking up past project parents).

    Returns a mapping of project Wrike ID → parent folder name.
    """
    project_ids: Set[str] = {p["id"] for p in raw_projects}
    project_by_id: Dict[str, Dict[str, Any]] = {p["id"]: p for p in raw_projects}

    # Walk up from each project to find the first non-project ancestor
    folder_ids_needed: Set[str] = set()
    project_to_folder_id: Dict[str, str] = {}

    def _walk_up(pid: str, seen: Set[str]) -> Optional[str]:
        if pid not in project_ids:
            return pid  # it's a folder
        if pid in seen:
            return None  # cycle guard
        seen.add(pid)
        parent = project_by_id.get(pid)
        if not parent:
            return None
        for gpid in parent.get("parentIds", []):
            result = _walk_up(gpid, seen)
            if result:
                return result
        return None

    for proj in raw_projects:
        for pid in proj.get("parentIds", []):
            if pid not in project_ids:
                # Direct parent is a folder
                folder_ids_needed.add(pid)
                project_to_folder_id[proj["id"]] = pid
            else:
                # Walk up to find the folder
                folder_id = _walk_up(pid, set())
                if folder_id:
                    folder_ids_needed.add(folder_id)
                    project_to_folder_id[proj["id"]] = folder_id

    if not folder_ids_needed:
        return {}

    # Batch fetch folder names from Wrike (up to 100 per request)
    folder_names: Dict[str, str] = {}
    folder_id_list = list(folder_ids_needed)
    for i in range(0, len(folder_id_list), 100):
        batch = folder_id_list[i:i + 100]
        ids_param = ",".join(batch)
        data = wrike.get(f"folders/{ids_param}")
        for folder in data.get("data", []):
            folder_names[folder["id"]] = folder.get("title", "")

    print(f"  Resolved {len(folder_names)} parent folder names")

    # Map project ID → folder name
    result: Dict[str, str] = {}
    for proj_id, folder_id in project_to_folder_id.items():
        name = folder_names.get(folder_id)
        if name:
            result[proj_id] = name

    return result


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
    """Run the projects step."""
    space_id = config["wrike"]["space_id"]
    projects_config = config["projects"]
    table_config = config["airtable"]["tables"]["projects"]

    # Fetch custom field definitions
    cf_names, cf_types = wrike.fetch_custom_fields()

    # Fetch status name mapping
    status_names = wrike.fetch_status_names()

    # Fetch or load raw projects
    if use_cache:
        raw_projects = load_json(cache_dir, "projects", "wrike_raw")
        if raw_projects is not None:
            print(f"  Loaded {len(raw_projects)} projects from cache")
        else:
            print("  No cache found, fetching from Wrike...")
            raw_projects = fetch_projects(wrike, space_id)
    else:
        raw_projects = fetch_projects(wrike, space_id)
        print(f"  Fetched {len(raw_projects)} projects from Wrike")

    # Always save raw data for inspectability
    save_json(cache_dir, "projects", "wrike_raw", raw_projects)

    # Resolve parent folder names if configured
    parent_folder_field = projects_config.get("parent_folder_field")
    if parent_folder_field:
        parent_folder_ref = field_ref(parent_folder_field)
        folder_names = _resolve_parent_folders(raw_projects, wrike)
    else:
        parent_folder_ref = None
        folder_names = {}

    if limit:
        raw_projects = raw_projects[:limit]
        print(f"  Limited to {limit} projects")

    # Transform each project using config-driven field mapping
    mapped = []
    for proj in raw_projects:
        record = apply_field_mapping(
            proj, projects_config, lookups, cf_names, cf_types, status_names
        )
        if record:
            # Inject parent folder name if resolved
            if parent_folder_ref and proj.get("id") in folder_names:
                record[parent_folder_ref] = folder_names[proj["id"]]
            mapped.append(record)

    print(f"  Transformed {len(mapped)} projects")

    # Save transformed data for inspectability
    save_json(cache_dir, "projects", "airtable_ready", mapped)

    # Upsert into Airtable
    upsert_key = field_refs(table_config["upsert_key"])
    upsert_result = airtable.upsert(
        table_config["table_id"],
        mapped,
        key_fields=upsert_key,
        dry_run=dry_run,
    )

    # Refresh the projects lookup so subsequent steps have fresh data
    if not dry_run:
        lookups.refresh("projects")

    return {
        "wrike_fetched": len(raw_projects),
        "transformed": len(mapped),
        **upsert_result,
    }
