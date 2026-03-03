# Wrike-to-Airtable Migration Runbook

## Prerequisites

1. **Python 3.10+** installed
2. **Dependencies** installed:
   ```bash
   cd wrike_migration
   pip install -r requirements.txt
   ```
3. **Environment variables** in `wrike_migration/.env`:
   ```
   WRIKE_API_TOKEN=your_wrike_token
   AIRTABLE_API_KEY=your_airtable_pat
   ```
4. **Config files** updated with actual IDs — replace all `FILL_IN` values in `configs/cs.json` and `configs/video.json`

## Config Setup

Each config file needs real Airtable IDs. Open your Airtable base and fill in:

| Config path | Where to find it |
|---|---|
| `wrike.space_id` | Wrike URL: `app-us2.wrike.com/open.htm?id=SPACE_ID` |
| `airtable.base_id` | Airtable URL: `airtable.com/appXXXXXX/...` |
| `airtable.tables.*.table_id` | Airtable table URL segment: `tblXXXXXX` |
| `airtable.lookups.*.table_id` | Same — table IDs for lookup tables (People, Statuses, etc.) |

### Field Mapping Format

All Airtable field references in `field_mapping`, `upsert_key`, `self_links`, and `dependencies` use the `{name, id}` format:

```json
"title": {"name": "Project Name", "id": "fldXXXXXXXXXXXXXX"}
```

- **`name`**: Human-readable field name (used for reading records from Airtable)
- **`id`**: Airtable field ID (used for writing records — more robust than names)

To find a field ID: open a table in Airtable, click a field header, then "Edit field" — the field ID is shown in the URL or field configuration panel (starts with `fld`).

While IDs remain `"FILL_IN"`, the pipeline falls back to using field names for writes. Replace with real IDs for production use.

### Timelogs Composite Upsert Key

Timelogs use a 3-field composite upsert key to uniquely identify each weekly entry:

```json
"upsert_key": [
  {"name": "Week Name", "id": "fldXXX"},
  {"name": "Person Wrike ID", "id": "fldXXX"},
  {"name": "Task Wrike ID", "id": "fldXXX"}
]
```

Your Airtable timelogs table must have these 3 text fields. Each timelog entry is keyed by the combination of week + person + task.

### Timelogs Additional Config

The timelogs section also supports:

```json
"timelogs": {
  "day_fields": ["S", "M", "T", "W", "Th", "F", "Sa"],
  "total_hours_field": {"name": "total_hours (wrike)", "id": "fldXXX"},
  "task_field": {"name": "Task", "id": "fldXXX"}
}
```

- **`total_hours_field`**: Field for the computed total hours per week/user/task
- **`task_field`**: Linked record field pointing to the tasks table (resolved via `tasks` lookup)

### Lookup Table Requirements

Each lookup table in Airtable must have the columns specified in `key_field` and `value_field`:

- **people**: Table with `Wrike ID` column (Wrike user IDs)
- **projects**: Same table as projects import — uses `Wrike ID (wrike)` column (CS) or `Wrike ID` (Video)
- **tasks**: Same table as tasks import — maps task Wrike IDs to record IDs (used by timelogs to link tasks)
- **status**: Statuses table with `Name` column (human-readable status name, matched via fuzzy matching)
- **weeks**: Weeks table with `Name` column (e.g., "Week 1 2026 (Jan 5 - 11)")
- **task_assignments**: Task assignments table with `Name` column

**Note on status resolution:** The pipeline fetches all workflow statuses from Wrike's `/workflows` API to build a statusId-to-name map. Opaque Wrike status IDs (e.g., `IEAAB7DSJMAAAAAA`) are resolved to human-readable names (e.g., "Active") before fuzzy-matching against the Airtable status table's `Name` field.

## Running the Pipeline

### Full Migration (All Steps)

```bash
cd wrike_migration
python migrate.py --config configs/cs.json --steps all
```

### Specific Steps Only

```bash
python migrate.py --config configs/cs.json --steps projects,tasks
```

### Resume from a Step

If the pipeline fails at a step, resume from that point:

```bash
python migrate.py --config configs/cs.json --from-step dependencies
```

### Dry Run (No Writes)

Preview what would happen without writing to Airtable:

```bash
python migrate.py --config configs/cs.json --steps all --dry-run
```

### Use Cached Data (Resume/Re-run)

Load from previously cached JSON files instead of re-fetching from APIs:

```bash
python migrate.py --config configs/cs.json --steps all --use-cache
```

This is useful when:
- A run was interrupted and you want to resume without re-fetching
- You want to re-run transforms without hitting API rate limits
- You want to inspect cached data, tweak config, and re-run

Cached data is saved to `data/cache/{space}/` on every run (overwritten each time, not date-partitioned). If a cache file doesn't exist, the step falls back to fetching from the API.

**Which steps support `--use-cache`:** tasks, attachments, and timelogs load from cached raw Wrike data. Other steps always fetch fresh data.

### Limit Records (Testing)

Process only N records per step for quick testing:

```bash
python migrate.py --config configs/cs.json --steps projects --dry-run --limit 5
```

## Step-by-Step Execution Order

| Step | Name | What it does |
|---|---|---|
| 1 | `projects` | Fetches projects from Wrike, transforms fields, upserts into Airtable |
| 2 | `project_links` | Resolves parent/child project Wrike IDs to Airtable record IDs |
| 3 | `tasks` | Fetches tasks + recursive subtasks, transforms, upserts |
| 4 | `task_links` | Resolves parent/subtask Wrike IDs to Airtable record IDs |
| 5 | `dependencies` | Fetches dependency details from Wrike, resolves predecessor/successor links |
| 6 | `attachments` | Fetches attachment URLs from Wrike (projects + tasks), updates Airtable attachment fields |
| 7 | `comments` | Fetches comments, formats with author/date, posts via Airtable comments API |
| 8 | `timelogs` | Fetches timelogs, aggregates by week/user/task, upserts |

## Re-Running for Updates

All steps use **upsert** (insert or update) keyed on the Wrike ID field. Safe to re-run anytime:

- New records are created
- Existing records are updated with fresh data
- No duplicates are created

**Comments** are idempotent: each imported comment includes a `[Imported from Wrike: COMMENT_ID]` marker. On re-run, existing comments are detected and skipped.

## Running Both Spaces

**Important: Run sequentially, not in parallel.** Running both spaces simultaneously causes Wrike API connection drops (`RemoteDisconnected` errors).

```bash
# CS space first
python migrate.py --config configs/cs.json --steps all

# Then Video space
python migrate.py --config configs/video.json --steps all
```

## Inspecting Results

### Cached Files

Every run saves intermediate JSON files to `data/cache/{space}/`:

```
data/cache/cs/
├── projects_wrike_raw.json        # Raw Wrike API response
├── projects_airtable_ready.json   # Transformed records sent to Airtable
├── tasks_wrike_raw.json
├── tasks_airtable_ready.json
├── dependencies_cache.json        # Dependency ID → predecessor/successor map
├── dependencies_airtable_ready.json
├── attachments_projects_airtable_ready.json
├── attachments_tasks_airtable_ready.json
├── comments_wrike_raw.json        # Per-task comments keyed by Wrike task ID
├── timelogs_wrike_raw.json
├── timelogs_airtable_ready.json
├── lookups_people.json            # Lookup table caches
├── lookups_projects.json
├── lookups_status.json
└── ...
```

Open `*_airtable_ready.json` to verify field mapping before running against production.

### Run Logs

Each pipeline run saves a summary to `data/logs/{space}/{timestamp}_run.json` containing:
- Config path, flags, and steps run
- Per-step results (fetched, transformed, created, updated counts)
- Timing information

### Pipeline Summary

At the end of each run, a summary table is printed:

```
============================================================
Pipeline Summary — CS
============================================================
  Step                 Fetched  Transformed   Created  Updated   Time
  ──────────────────────────────────────────────────────────
  projects                 150          142       140        2   3.2s
  tasks                   1200         1180      1150       30  45.2s
  ...
============================================================
```

## Troubleshooting

### Rate Limiting (429 errors)

The pipeline automatically handles Wrike's 400 req/min rate limit with exponential backoff. If you see repeated 429s, the pipeline will retry up to 5 times per request.

### Missing Lookup Entries

If you see "Linked field lookup miss" debug messages, it means a Wrike ID couldn't be resolved to an Airtable record ID. This usually means:
- The referenced record hasn't been imported yet (run steps in order)
- The lookup table is missing entries (check the lookup table in Airtable)

### Step Failures

If a step fails, the pipeline prints a resume command:
```
Stopping pipeline. Resume with: --from-step <step_name>
```

Fix the issue and re-run with that flag.

### Custom Field Mismatches

If custom fields aren't mapping correctly:
1. Check that the custom field name in the config matches the Wrike custom field name exactly (case-sensitive, including brackets)
2. Run with `--limit 1 --dry-run` to see the raw data and transformed output

### Attachment URL Expiration

Wrike attachment URLs are signed JWTs with an expiration. If you use `--use-cache` for attachments and too much time has passed since the original fetch, the URLs may be expired and Airtable will reject them. In that case, re-run without `--use-cache` to get fresh URLs.

### Dependency Caching

Dependency details are cached in `data/cache/{space}/dependencies_cache.json`. To force re-fetch, delete this file.

### Clearing Cached Data

To force all steps to re-fetch from APIs, delete the space's cache directory:

```bash
rm -rf wrike_migration/data/cache/cs/
```

### Graceful Shutdown

During the comments step, press Ctrl+C once to finish the current task and stop cleanly. Press Ctrl+C twice to force stop.
