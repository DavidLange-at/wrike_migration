"""JSON cache helpers for saving/loading intermediate pipeline data."""

import json
import os
from typing import Any, Optional, Union


def cache_path(cache_dir: str, step: str, stage: str) -> str:
    """Return the cache file path: {cache_dir}/{step}_{stage}.json."""
    return os.path.join(cache_dir, f"{step}_{stage}.json")


def save_json(cache_dir: str, step: str, stage: str, data: Any) -> str:
    """Save data to {cache_dir}/{step}_{stage}.json. Returns the file path."""
    os.makedirs(cache_dir, exist_ok=True)
    path = cache_path(cache_dir, step, stage)
    with open(path, "w") as f:
        json.dump(data, f, indent=2, default=str)
    print(f"    Saved {_describe(data)} to {os.path.basename(path)}")
    return path


def load_json(cache_dir: str, step: str, stage: str) -> Optional[Union[list, dict]]:
    """Load from cache file. Returns None if file doesn't exist."""
    path = cache_path(cache_dir, step, stage)
    if not os.path.exists(path):
        return None
    with open(path, "r") as f:
        return json.load(f)


def _describe(data: Any) -> str:
    """Short description of data for log messages."""
    if isinstance(data, list):
        return f"{len(data)} records"
    if isinstance(data, dict):
        return f"{len(data)} entries"
    return "data"
