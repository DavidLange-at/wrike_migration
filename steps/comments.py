# wrike_migration/steps/comments.py
"""
Step 7: Fetch comments from Wrike and import via Airtable native comments API.

Idempotency: checks existing comments for the [Imported from Wrike: ID] marker.
"""

import html
import signal
import sys
import time
from datetime import datetime
from typing import Any, Dict, List, Optional

from markdownify import markdownify as md

from core import field_name
from core.cache import save_json, load_json
from core.wrike_client import WrikeClient
from core.airtable_client import AirtableClient
from core.lookups import LookupManager


# Graceful shutdown support
_shutdown_requested = False


def _signal_handler(signum, frame):
    global _shutdown_requested
    if _shutdown_requested:
        sys.exit(1)
    _shutdown_requested = True
    print("\n  Shutdown requested. Finishing current task...")


def _format_comment(
    comment: Dict[str, Any],
    author_name: str,
    template: str,
) -> str:
    """Format a Wrike comment for Airtable using the config template."""
    html_text = comment.get("text", "")
    decoded = html.unescape(html_text) if html_text else ""
    markdown_text = md(decoded, strip=["span"], heading_style="ATX").strip()

    created = comment.get("createdDate", "")
    try:
        dt = datetime.fromisoformat(created.replace("Z", "+00:00"))
        formatted_date = dt.strftime("%b %d, %Y %I:%M %p").replace(" 0", " ")
    except (ValueError, AttributeError):
        formatted_date = created

    return template.format(
        author=author_name,
        date=formatted_date,
        text=markdown_text,
        comment_id=comment.get("id", ""),
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
    """Fetch Wrike comments and post to Airtable."""
    global _shutdown_requested
    _shutdown_requested = False

    old_handler = signal.getsignal(signal.SIGINT)
    signal.signal(signal.SIGINT, _signal_handler)

    try:
        return _run_inner(config, wrike, airtable, lookups, dry_run, limit, cache_dir, use_cache)
    finally:
        signal.signal(signal.SIGINT, old_handler)


def _run_inner(
    config: Dict[str, Any],
    wrike: WrikeClient,
    airtable: AirtableClient,
    lookups: LookupManager,
    dry_run: bool,
    limit: Optional[int],
    cache_dir: str = "",
    use_cache: bool = False,
) -> Dict[str, Any]:
    global _shutdown_requested

    comments_config = config.get("comments", {})
    template = comments_config.get(
        "format_template",
        "Originally posted by {author}\n{date}\n\n{text}\n\n---\n[Imported from Wrike: {comment_id}]"
    )

    table_config = config["airtable"]["tables"]["tasks"]
    table_id = table_config["table_id"]
    upsert_key_name = field_name(table_config["upsert_key"])

    # Fetch all tasks from Airtable
    print("  Fetching tasks from Airtable...")
    records = airtable.fetch_all(table_id, fields=[upsert_key_name])
    task_wrike_ids = []
    wrike_to_record: Dict[str, str] = {}
    for rec in records:
        fields = rec.get("fields", {})
        wrike_id = fields.get(upsert_key_name)
        if wrike_id:
            task_wrike_ids.append(wrike_id)
            wrike_to_record[wrike_id] = rec["id"]

    print(f"  {len(task_wrike_ids)} tasks to check for comments")

    # Build user name lookup from people config
    people_config = config["airtable"]["lookups"]["people"]
    people_table = people_config["table_id"]
    people_key_field = people_config["key_field"]
    people_name_field = people_config.get("name_field", "Full Name")
    people_records = airtable.fetch_all(people_table, fields=[people_key_field, people_name_field])
    user_names: Dict[str, str] = {}
    for rec in people_records:
        fields = rec.get("fields", {})
        wrike_id = fields.get(people_key_field)
        name = fields.get(people_name_field)
        if wrike_id and name:
            user_names[str(wrike_id)] = name

    stats = {"fetched": 0, "imported": 0, "skipped_existing": 0, "skipped_error": 0}

    # Load or prepare comment cache
    cached_comments: Dict[str, list] = {}
    if use_cache:
        cached_comments = load_json(cache_dir, "comments", "wrike_raw") or {}
        if cached_comments:
            print(f"  Loaded cached comments for {len(cached_comments)} tasks")

    all_comments_raw: Dict[str, list] = dict(cached_comments)

    total = min(len(task_wrike_ids), limit) if limit else len(task_wrike_ids)

    for idx, task_wrike_id in enumerate(task_wrike_ids[:total], 1):
        if _shutdown_requested:
            print(f"\n  Stopped at {idx}/{total}. {stats}")
            break

        record_id = wrike_to_record[task_wrike_id]

        # Fetch comments from Wrike (or use cache)
        if task_wrike_id in all_comments_raw:
            comments = all_comments_raw[task_wrike_id]
        else:
            try:
                data = wrike.get(f"tasks/{task_wrike_id}/comments")
                comments = data.get("data", [])
            except Exception:
                stats["skipped_error"] += 1
                continue

            if not comments:
                continue

            all_comments_raw[task_wrike_id] = comments

        if not comments:
            continue

        # Check existing comments for already-imported markers
        existing_comments = airtable.list_comments(table_id, record_id)
        existing_markers = set()
        for ec in existing_comments:
            text = ec.text or ""
            if "[Imported from Wrike:" in text:
                start = text.index("[Imported from Wrike:") + len("[Imported from Wrike:")
                end = text.index("]", start)
                existing_markers.add(text[start:end].strip())

        # Sort by date and post
        comments.sort(key=lambda c: c.get("createdDate", ""))

        for comment in comments:
            comment_id = comment.get("id", "")
            stats["fetched"] += 1

            if comment_id in existing_markers:
                stats["skipped_existing"] += 1
                continue

            author_id = comment.get("authorId", "")
            author_name = user_names.get(author_id, "Unknown User")
            formatted = _format_comment(comment, author_name, template)

            success = airtable.add_comment(table_id, record_id, formatted, dry_run=dry_run)
            if success:
                stats["imported"] += 1
            else:
                stats["skipped_error"] += 1

            time.sleep(0.25)

        if idx % 100 == 0:
            print(f"    Progress: {idx}/{total} tasks | {stats}")

    # Save raw comments for inspectability
    if all_comments_raw:
        save_json(cache_dir, "comments", "wrike_raw", all_comments_raw)

    print(f"\n  Comments complete: {stats}")
    return stats
