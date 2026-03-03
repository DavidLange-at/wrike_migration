# wrike_migration/steps/projects.py
"""
Step 1: Fetch projects from Wrike, transform, and upsert into Airtable.
"""

from typing import Any, Dict, List, Optional

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
