# Wrike Migration

Config-driven pipeline for migrating data from Wrike to Airtable. Handles projects, tasks (with recursive subtasks), dependencies, attachments, comments, and timelogs across multiple Wrike spaces.

## Features

- **Config-driven**: One JSON config per Wrike space defines all field mappings, lookups, and transforms
- **8-step pipeline**: Projects, project links, tasks, task links, dependencies, attachments, comments, timelogs
- **Upsert-based**: Safe to re-run — creates new records, updates existing ones, no duplicates
- **Caching**: Intermediate JSON saved per step; `--use-cache` resumes without re-fetching from APIs
- **Dry run**: Preview all transforms without writing to Airtable
- **Rate limiting**: Automatic Wrike API rate limiting with exponential backoff on 429s
- **Run logs**: Per-run JSON logs with timing and record counts

## Quick Start

```bash
pip install -r requirements.txt
cp .env.example .env
# Fill in WRIKE_API_TOKEN and AIRTABLE_API_KEY in .env
# Fill in Airtable field IDs in configs/cs.json and/or configs/video.json
```

```bash
# Dry run
python migrate.py --config configs/cs.json --steps all --dry-run

# Full migration
python migrate.py --config configs/cs.json --steps all

# Resume from a step
python migrate.py --config configs/cs.json --from-step tasks

# Re-run with cached Wrike data
python migrate.py --config configs/cs.json --steps all --use-cache
```

See [RUNBOOK.md](RUNBOOK.md) for full setup instructions, config format, and troubleshooting.

## Project Structure

```
├── migrate.py              # CLI entry point
├── configs/
│   ├── cs.json             # CS space config
│   └── video.json          # Video space config
├── core/
│   ├── __init__.py         # Config helpers (field_ref, field_name)
│   ├── wrike_client.py     # Wrike API client with rate limiting
│   ├── airtable_client.py  # Airtable upsert + comments
│   ├── lookups.py          # Lookup table manager (people, projects, statuses)
│   ├── transforms.py       # Field mapping engine
│   └── cache.py            # JSON cache helpers
├── steps/                  # One module per pipeline step
│   ├── projects.py         # Step 1: Fetch & upsert projects
│   ├── project_links.py    # Step 2: Resolve project parent/child links
│   ├── tasks.py            # Step 3: Fetch tasks + subtasks, upsert
│   ├── task_links.py       # Step 4: Resolve task parent/subtask links
│   ├── dependencies.py     # Step 5: Fetch & resolve task dependencies
│   ├── attachments.py      # Step 6: Fetch attachment URLs, upsert
│   ├── comments.py         # Step 7: Fetch & post comments
│   └── timelogs.py         # Step 8: Fetch timelogs, aggregate weekly, upsert
└── data/
    ├── cache/{space}/      # Intermediate JSON (gitignored)
    └── logs/{space}/       # Run logs (gitignored)
```
