"""
Wrike API client with rate limiting, exponential backoff, and pagination.

Consolidates duplicated request logic from the standalone export scripts
into a single reusable client.
"""

import logging
import os
import time
from typing import Any, Optional

import requests
from dotenv import load_dotenv

logger = logging.getLogger(__name__)


class WrikeClient:
    """Wrike API client with built-in rate limiting and retry logic."""

    def __init__(
        self,
        base_url: str = "https://app-us2.wrike.com/api/v4",
        rate_limit_per_minute: int = 400,
        max_retries: int = 5,
        timeout: int = 60,
    ) -> None:
        load_dotenv()
        token = os.getenv("WRIKE_API_TOKEN")
        if not token:
            raise ValueError("WRIKE_API_TOKEN not found in environment")

        self.base_url = base_url.rstrip("/")
        self.headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        }
        self._interval = 60.0 / rate_limit_per_minute
        self._last_request_ts = 0.0
        self._max_retries = max_retries
        self._timeout = timeout

    # ------------------------------------------------------------------
    # Rate limiting
    # ------------------------------------------------------------------

    def _wait_for_rate_limit(self) -> None:
        """Sleep if needed to maintain the configured request interval."""
        elapsed = time.time() - self._last_request_ts
        wait = self._interval - elapsed
        if wait > 0:
            time.sleep(wait)
        self._last_request_ts = time.time()

    # ------------------------------------------------------------------
    # Core request method
    # ------------------------------------------------------------------

    def get(
        self, path: str, params: Optional[dict[str, Any]] = None
    ) -> dict[str, Any]:
        """Make a GET request with rate limiting and exponential backoff on 429.

        Args:
            path: API path (e.g. "/customfields" or "folders/ID/tasks").
            params: Optional query parameters.

        Returns:
            Parsed JSON response as a dict.

        Raises:
            requests.HTTPError: On non-retryable HTTP errors or after
                exhausting all retries on 429.
        """
        url = f"{self.base_url}/{path.lstrip('/')}"
        attempt = 0

        while True:
            self._wait_for_rate_limit()
            response = requests.get(
                url,
                headers=self.headers,
                params=params,
                timeout=self._timeout,
            )

            if response.status_code != 429:
                response.raise_for_status()
                return response.json()

            attempt += 1
            if attempt > self._max_retries:
                response.raise_for_status()

            retry_after = response.headers.get("Retry-After")
            try:
                sleep_for = (
                    float(retry_after) if retry_after else 2 ** (attempt - 1)
                )
            except ValueError:
                sleep_for = 2 ** (attempt - 1)

            logger.warning(
                "429 rate limited on %s. Sleeping %.1fs (attempt %d/%d)",
                url,
                sleep_for,
                attempt,
                self._max_retries,
            )
            print(
                f"  429 rate limited. Sleeping {sleep_for:.1f}s "
                f"(attempt {attempt}/{self._max_retries})"
            )
            time.sleep(sleep_for)

    # ------------------------------------------------------------------
    # Pagination
    # ------------------------------------------------------------------

    def get_paginated(
        self,
        path: str,
        params: Optional[dict[str, str]] = None,
        page_size: int = 200,
    ) -> list[dict[str, Any]]:
        """Fetch all pages from a paginated Wrike endpoint.

        Handles the ``nextPageToken`` pattern used by Wrike's API.  Logs
        progress after every page.

        Args:
            path: API path (e.g. "spaces/SPACEID/folders").
            params: Extra query parameters merged into each request.
            page_size: Number of items per page (Wrike max varies by
                endpoint; 1000 for tasks, 200 for folders).

        Returns:
            Flat list of all ``data`` items across every page.
        """
        all_data: list[dict[str, Any]] = []
        full_params: dict[str, Any] = dict(params or {})
        full_params["pageSize"] = str(page_size)
        next_token: Optional[str] = None
        page = 1

        while True:
            if next_token:
                full_params["nextPageToken"] = next_token
            elif "nextPageToken" in full_params:
                del full_params["nextPageToken"]

            data = self.get(path, params=full_params)
            page_items = data.get("data", [])
            all_data.extend(page_items)

            print(
                f"    Page {page}: {len(page_items)} items "
                f"(total: {len(all_data)})"
            )
            logger.info(
                "Page %d: %d items (total: %d)",
                page,
                len(page_items),
                len(all_data),
            )

            next_token = data.get("nextPageToken")
            if not next_token or not page_items:
                break
            page += 1

        return all_data

    # ------------------------------------------------------------------
    # Convenience helpers
    # ------------------------------------------------------------------

    def fetch_custom_fields(self) -> tuple[dict[str, str], dict[str, str]]:
        """Fetch all custom fields from Wrike.

        Returns:
            ``(id_to_name, id_to_type)`` dictionaries mapping custom field
            IDs to their human-readable title and field type respectively.
        """
        data = self.get("/customfields")
        id_to_name: dict[str, str] = {}
        id_to_type: dict[str, str] = {}
        for field in data.get("data", []):
            field_id = field.get("id")
            title = field.get("title")
            field_type = field.get("type")
            if field_id and title:
                id_to_name[field_id] = title
            if field_id and field_type:
                id_to_type[field_id] = field_type
        print(f"  Fetched {len(id_to_name)} custom fields from Wrike API")
        logger.info("Fetched %d custom fields from Wrike API", len(id_to_name))
        return id_to_name, id_to_type

    def fetch_status_names(self) -> dict[str, str]:
        """Fetch all workflow statuses from Wrike.

        Returns:
            Mapping of ``customStatusId`` to human-readable status name.
        """
        data = self.get("/workflows")
        status_map: dict[str, str] = {}
        for workflow in data.get("data", []):
            for status in workflow.get("customStatuses", []):
                sid = status.get("id")
                name = status.get("name")
                if sid and name:
                    status_map[sid] = name
        print(f"  Fetched {len(status_map)} status names from Wrike workflows")
        logger.info("Fetched %d status names from Wrike workflows", len(status_map))
        return status_map
