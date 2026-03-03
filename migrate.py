#!/usr/bin/env python3
"""
Wrike-to-Airtable Migration Pipeline

Usage:
    python migrate.py --config configs/cs.json --steps all
    python migrate.py --config configs/cs.json --steps projects,tasks
    python migrate.py --config configs/cs.json --from-step tasks
    python migrate.py --config configs/cs.json --steps projects --dry-run
    python migrate.py --config configs/cs.json --steps tasks --limit 10
"""

import argparse
import json
import os
import sys
import time
from datetime import datetime
from typing import Any, Dict, List

from core.wrike_client import WrikeClient
from core.airtable_client import AirtableClient
from core.lookups import LookupManager
from steps import (
    projects,
    project_links,
    tasks,
    task_links,
    dependencies,
    attachments,
    comments,
    timelogs,
)


# Ordered list of all steps
STEP_ORDER = [
    "projects",
    "project_links",
    "tasks",
    "task_links",
    "dependencies",
    "attachments",
    "comments",
    "timelogs",
]

STEP_MODULES = {
    "projects": projects,
    "project_links": project_links,
    "tasks": tasks,
    "task_links": task_links,
    "dependencies": dependencies,
    "attachments": attachments,
    "comments": comments,
    "timelogs": timelogs,
}

# Steps that need a WrikeClient
WRIKE_STEPS = {"projects", "tasks", "dependencies", "attachments", "comments", "timelogs"}


def load_config(config_path: str) -> Dict[str, Any]:
    with open(config_path, "r") as f:
        return json.load(f)


def resolve_steps(args) -> List[str]:
    """Determine which steps to run based on CLI args."""
    if args.from_step:
        if args.from_step not in STEP_ORDER:
            print(f"Error: unknown step '{args.from_step}'. Valid: {STEP_ORDER}")
            sys.exit(1)
        idx = STEP_ORDER.index(args.from_step)
        return STEP_ORDER[idx:]

    if args.steps == "all":
        return list(STEP_ORDER)

    steps = [s.strip() for s in args.steps.split(",")]
    for s in steps:
        if s not in STEP_ORDER:
            print(f"Error: unknown step '{s}'. Valid: {STEP_ORDER}")
            sys.exit(1)
    return steps


def main():
    parser = argparse.ArgumentParser(description="Wrike-to-Airtable Migration Pipeline")
    parser.add_argument("--config", required=True, help="Path to space config JSON")
    parser.add_argument("--steps", default="all", help="Comma-separated steps or 'all'")
    parser.add_argument("--from-step", default=None, help="Run from this step onward")
    parser.add_argument("--dry-run", action="store_true", help="Log actions without writing")
    parser.add_argument("--limit", type=int, default=None, help="Limit records per step")
    parser.add_argument("--use-cache", action="store_true", help="Load from cached data instead of API")
    args = parser.parse_args()

    # Load config
    config = load_config(args.config)
    space_name = config.get("space_name", "unknown")
    print(f"{'=' * 60}")
    print(f"Wrike Migration Pipeline — {space_name}")
    print(f"{'=' * 60}")
    if args.dry_run:
        print("*** DRY RUN MODE ***")
    if args.use_cache:
        print("*** USING CACHED DATA ***")
    print()

    # Resolve steps
    steps = resolve_steps(args)
    print(f"Steps to run: {steps}\n")

    # Initialize clients
    wrike = None
    if any(s in WRIKE_STEPS for s in steps):
        wrike_config = config.get("wrike", {})
        wrike = WrikeClient(
            base_url=wrike_config.get("base_url", "https://app-us2.wrike.com/api/v4")
        )

    airtable = AirtableClient(base_id=config["airtable"]["base_id"])

    # Set up data directories (space-partitioned)
    script_dir = os.path.dirname(os.path.abspath(__file__))
    data_dir = os.path.join(script_dir, "data")
    cache_dir = os.path.join(data_dir, "cache", space_name)
    log_dir = os.path.join(data_dir, "logs", space_name)
    os.makedirs(cache_dir, exist_ok=True)
    os.makedirs(log_dir, exist_ok=True)

    lookups = LookupManager(
        airtable,
        config["airtable"].get("lookups", {}),
        cache_dir=cache_dir,
        use_cache=args.use_cache,
    )

    # Run steps
    total_start = time.time()
    step_results = {}

    for step_name in steps:
        print(f"\n{'─' * 60}")
        print(f"Step: {step_name}")
        print(f"{'─' * 60}")
        step_start = time.time()

        module = STEP_MODULES[step_name]
        kwargs: Dict[str, Any] = {
            "config": config,
            "airtable": airtable,
            "lookups": lookups,
            "dry_run": args.dry_run,
            "limit": args.limit,
            "data_dir": data_dir,
            "cache_dir": cache_dir,
            "use_cache": args.use_cache,
        }
        if step_name in WRIKE_STEPS:
            kwargs["wrike"] = wrike

        try:
            result = module.run(**kwargs) or {}
        except Exception as e:
            print(f"\n  ERROR in step '{step_name}': {e}")
            import traceback
            traceback.print_exc()
            print(f"\n  Stopping pipeline. Resume with: --from-step {step_name}")
            sys.exit(1)

        elapsed = time.time() - step_start
        result["elapsed"] = round(elapsed, 1)
        step_results[step_name] = result

        # Per-step summary line
        parts = []
        for key in ("wrike_fetched", "resolved", "fetched", "transformed", "imported",
                    "created", "updated", "skipped_existing", "skipped_error"):
            if key in result:
                label = key.replace("_", " ")
                parts.append(f"{result[key]} {label}")
        summary_line = ", ".join(parts) if parts else "done"
        print(f"\n  -> {step_name}: {summary_line} ({elapsed:.1f}s)")

    total_elapsed = time.time() - total_start

    # Print summary table
    print(f"\n{'=' * 60}")
    print(f"Pipeline Summary — {space_name}")
    print(f"{'=' * 60}")
    header = f"  {'Step':<20} {'Fetched':>8} {'Transformed':>12} {'Created':>8} {'Updated':>8} {'Time':>6}"
    print(header)
    print(f"  {'─' * 58}")

    total_created = 0
    total_updated = 0
    for name, res in step_results.items():
        fetched = res.get("wrike_fetched", res.get("resolved", res.get("fetched", "-")))
        transformed = res.get("transformed", "-")
        created = res.get("created", "-")
        updated = res.get("updated", "-")
        elapsed = res.get("elapsed", 0)

        # For comments, show fetched/imported
        if "imported" in res:
            fetched = f"{res.get('fetched', 0)}/{res.get('imported', 0)}"

        if isinstance(created, int):
            total_created += created
        if isinstance(updated, int):
            total_updated += updated

        print(f"  {name:<20} {str(fetched):>8} {str(transformed):>12} {str(created):>8} {str(updated):>8} {elapsed:>5.1f}s")

    print(f"  {'─' * 58}")
    print(f"  {'TOTAL':<20} {'':>8} {'':>12} {total_created:>8} {total_updated:>8} {total_elapsed:>5.1f}s")
    print(f"{'=' * 60}")

    # Save run log
    run_log = {
        "space": space_name,
        "config_path": args.config,
        "dry_run": args.dry_run,
        "use_cache": args.use_cache,
        "limit": args.limit,
        "steps_run": steps,
        "started_at": datetime.now().isoformat(),
        "total_elapsed": round(total_elapsed, 1),
        "results": step_results,
    }
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_path = os.path.join(log_dir, f"{timestamp}_run.json")
    with open(log_path, "w") as f:
        json.dump(run_log, f, indent=2, default=str)
    print(f"\nRun log saved to {log_path}")


if __name__ == "__main__":
    main()
