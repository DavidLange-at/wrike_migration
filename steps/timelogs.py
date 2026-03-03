# wrike_migration/steps/timelogs.py
"""
Step 8: Fetch timelogs from Wrike, aggregate weekly, resolve lookups, upsert.
"""

import re
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from core import field_ref, field_name, field_refs, field_names
from core.wrike_client import WrikeClient
from core.airtable_client import AirtableClient
from core.lookups import LookupManager
from core.cache import save_json, load_json


def _compute_week_range(tracked_date: datetime) -> str:
    """Compute week range string matching Airtable weeks format."""
    monday = tracked_date - timedelta(days=tracked_date.weekday())
    sunday = monday + timedelta(days=6)

    # Week 1 starts on the first Monday on or after Jan 1
    year_start = datetime(monday.year, 1, 1)
    days_until_monday = (7 - year_start.weekday()) % 7
    first_monday = year_start + timedelta(days=days_until_monday)

    week_num = max(1, (monday - first_monday).days // 7 + 1)

    monday_month = monday.strftime("%b")
    sunday_month = sunday.strftime("%b")

    if monday.month == sunday.month:
        return f"Week {week_num} {monday.year} ({monday_month} {monday.day} - {sunday.day})"
    return f"Week {week_num} {monday.year} ({monday_month} {monday.day} - {sunday_month} {sunday.day})"


def _parse_tracked_date(date_str: str) -> Optional[datetime]:
    """Parse various date formats from Wrike timelogs."""
    if not date_str:
        return None
    try:
        if "T" in date_str:
            return datetime.fromisoformat(date_str.replace("Z", "+00:00"))
        elif "-" in date_str:
            return datetime.strptime(date_str, "%Y-%m-%d")
        elif "/" in date_str:
            try:
                return datetime.strptime(date_str, "%m/%d/%y")
            except ValueError:
                return datetime.strptime(date_str, "%m/%d/%Y")
    except ValueError:
        return None
    return None


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
    """Fetch timelogs, aggregate weekly, and upsert."""
    timelogs_config = config.get("timelogs", {})
    table_config = config["airtable"]["tables"].get("timelogs")
    if not table_config:
        print("  No timelogs table configured, skipping.")
        return {}

    table_id = table_config["table_id"]
    upsert_keys = field_refs(table_config["upsert_key"])
    tasks_table = config["airtable"]["tables"]["tasks"]

    # The upsert key config defines 3 fields: week name, person wrike id, task wrike id.
    # Map them by position.
    week_key_ref = upsert_keys[0] if len(upsert_keys) > 0 else "Week Name"
    person_key_ref = upsert_keys[1] if len(upsert_keys) > 1 else "Person Wrike ID"
    task_key_ref = upsert_keys[2] if len(upsert_keys) > 2 else "Task Wrike ID"

    total_hours_ref = field_ref(timelogs_config["total_hours_field"]) if "total_hours_field" in timelogs_config else "total_hours"
    task_field_ref = field_ref(timelogs_config["task_field"]) if "task_field" in timelogs_config else None

    # Fetch task Wrike IDs from Airtable
    print("  Fetching task list from Airtable...")
    tasks_upsert_name = field_name(tasks_table["upsert_key"])
    task_records = airtable.fetch_all(
        tasks_table["table_id"],
        fields=[tasks_upsert_name],
    )
    task_wrike_ids = [
        rec["fields"][tasks_upsert_name]
        for rec in task_records
        if rec.get("fields", {}).get(tasks_upsert_name)
    ]
    print(f"  {len(task_wrike_ids)} tasks to fetch timelogs for")

    if limit:
        task_wrike_ids = task_wrike_ids[:limit]

    # Fetch or load timelogs
    all_timelogs: List[Dict[str, Any]] = []
    seen_ids = set()

    if use_cache:
        cached = load_json(cache_dir, "timelogs", "wrike_raw")
        if cached is not None:
            all_timelogs = cached
            seen_ids = {t.get("id") for t in cached}
            print(f"  Loaded {len(all_timelogs)} timelogs from cache")

    if not all_timelogs:
        for idx, task_id in enumerate(task_wrike_ids, 1):
            try:
                data = wrike.get_paginated(f"tasks/{task_id}/timelogs")
                for tl in data:
                    tl_id = tl.get("id")
                    if tl_id and tl_id not in seen_ids:
                        seen_ids.add(tl_id)
                        all_timelogs.append(tl)
            except Exception as e:
                if "404" not in str(e):
                    print(f"    Error for task {task_id}: {e}")

            if idx % 100 == 0:
                print(f"    Fetched timelogs for {idx}/{len(task_wrike_ids)} tasks ({len(all_timelogs)} timelogs)")

    print(f"  Total: {len(all_timelogs)} unique timelogs")

    # Save raw data
    save_json(cache_dir, "timelogs", "wrike_raw", all_timelogs)

    # Aggregate by week/user/task
    day_names = ["M", "T", "W", "Th", "F", "Sa", "S"]  # Monday=0 to Sunday=6
    weekly_data = defaultdict(lambda: {
        "S": 0, "M": 0, "T": 0, "W": 0, "Th": 0, "F": 0, "Sa": 0,
        "task_id": "", "user_id": "", "week_start": "", "week_range": "",
    })

    for tl in all_timelogs:
        tracked_date = _parse_tracked_date(tl.get("trackedDate", ""))
        if not tracked_date:
            continue

        task_id = tl.get("taskId", "")
        user_id = tl.get("userId", "")
        monday = tracked_date - timedelta(days=tracked_date.weekday())
        week_start = monday.strftime("%Y-%m-%d")
        week_range = _compute_week_range(tracked_date)

        key = f"{week_start}|{user_id}|{task_id}"
        day_col = day_names[tracked_date.weekday()]
        hours = float(tl.get("hours", 0)) if tl.get("hours") else 0

        weekly_data[key][day_col] += hours
        weekly_data[key]["task_id"] = task_id
        weekly_data[key]["user_id"] = user_id
        weekly_data[key]["week_start"] = week_start
        weekly_data[key]["week_range"] = week_range

    # Resolve lookups and build output
    people_lookup = lookups.get("people")
    weeks_lookup = lookups.get("weeks") if "weeks" in config["airtable"].get("lookups", {}) else {}
    tasks_lookup = lookups.get("tasks") if "tasks" in config["airtable"].get("lookups", {}) else {}
    assignments_lookup = lookups.get("task_assignments") if "task_assignments" in config["airtable"].get("lookups", {}) else {}

    # Build fallback lookup keyed by "YYYY (date range)" to handle
    # years where week numbering differs from our computation.
    weeks_by_date_range: Dict[str, str] = {}
    for week_name, record_id in weeks_lookup.items():
        m = re.match(r"Week \d+ (\d{4} \(.+\))", week_name)
        if m:
            weeks_by_date_range.setdefault(m.group(1), record_id)

    output: List[Dict[str, Any]] = []
    day_fields = timelogs_config.get("day_fields", ["S", "M", "T", "W", "Th", "F", "Sa"])

    for key, data in weekly_data.items():
        total_hours = sum(data[d] for d in day_fields)

        entry: Dict[str, Any] = {}

        # Composite upsert key fields
        entry[week_key_ref] = data["week_range"]
        entry[person_key_ref] = data["user_id"]
        entry[task_key_ref] = data["task_id"]

        # Resolve week — try exact name first, then fall back to date range match
        week_record = weeks_lookup.get(data["week_range"]) if weeks_lookup else None
        if not week_record:
            m = re.match(r"Week \d+ (\d{4} \(.+\))", data["week_range"])
            if m:
                week_record = weeks_by_date_range.get(m.group(1))
        if week_record:
            entry["Weeks"] = [week_record]

        # Resolve person
        person_record = people_lookup.get(data["user_id"])
        if person_record:
            entry["Person"] = [person_record]

        # Resolve task
        if task_field_ref:
            task_record = tasks_lookup.get(data["task_id"])
            if task_record:
                entry[task_field_ref] = [task_record]

        # Resolve task assignment
        assignment_key = f"{data['task_id']}|{data['user_id']}"
        assignment_record = assignments_lookup.get(assignment_key) if assignments_lookup else None
        if assignment_record:
            entry["Task Assignment"] = [assignment_record]

        # Day columns
        for d in day_fields:
            if data[d] > 0:
                entry[d] = round(data[d], 2)

        if total_hours > 0:
            entry[total_hours_ref] = round(total_hours, 2)

        if entry:
            output.append(entry)

    output.sort(key=lambda x: (str(x.get("Weeks", "")), str(x.get("Person", ""))))
    print(f"  Aggregated {len(output)} weekly timelog entries")
    save_json(cache_dir, "timelogs", "airtable_ready", output)

    upsert_result = airtable.upsert(
        table_id,
        output,
        key_fields=upsert_keys,
        dry_run=dry_run,
    )

    return {
        "wrike_fetched": len(all_timelogs),
        "transformed": len(output),
        **upsert_result,
    }
