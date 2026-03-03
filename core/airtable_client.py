# wrike_migration/core/airtable_client.py
"""
Airtable client for batch upserts and native comments API.

Wraps pyairtable for structured table operations and uses urllib.request
for the native comments API (which pyairtable does not cover).
"""

import os
import time
from typing import Any, Dict, List, Optional

from dotenv import load_dotenv
from pyairtable import Api


class AirtableClient:
    """Airtable client wrapping pyairtable + native comments API."""

    def __init__(self, base_id: str):
        load_dotenv()
        self._api_key = os.getenv("AIRTABLE_API_KEY")
        if not self._api_key:
            raise ValueError("AIRTABLE_API_KEY not found in environment")

        self._api = Api(self._api_key)
        self.base_id = base_id
        self._last_api_call = 0.0
        self._api_interval = 0.2  # 5 req/sec Airtable limit

    def _table(self, table_id: str):
        """Return a pyairtable Table object for the given table."""
        return self._api.table(self.base_id, table_id)

    def _rate_limit_sleep(self) -> None:
        """Sleep if needed to stay under Airtable's 5 req/sec limit."""
        elapsed = time.time() - self._last_api_call
        wait = self._api_interval - elapsed
        if wait > 0:
            time.sleep(wait)
        self._last_api_call = time.time()

    # ------------------------------------------------------------------
    # Record operations
    # ------------------------------------------------------------------

    def upsert(
        self,
        table_id: str,
        records: List[Dict[str, Any]],
        key_fields: List[str],
        dry_run: bool = False,
    ) -> Dict[str, int]:
        """Batch upsert records into an Airtable table.

        Args:
            table_id: The Airtable table ID or name.
            records: List of field dicts (NOT wrapped in ``{"fields": ...}``).
            key_fields: Fields to match on for upsert.
            dry_run: If True, log what would happen but don't write.

        Returns:
            ``{"created": N, "updated": N}`` counts.
        """
        wrapped = [{"fields": r} for r in records]

        if dry_run:
            print(f"  [DRY RUN] Would upsert {len(wrapped)} records into {table_id}")
            return {"created": 0, "updated": 0}

        table = self._table(table_id)
        result = table.batch_upsert(wrapped, key_fields=key_fields, typecast=True)

        created = len(result.get("createdRecords", []))
        updated = len(result.get("updatedRecords", []))
        print(f"  Upserted {created + updated} records ({created} created, {updated} updated)")
        return {"created": created, "updated": updated}

    def fetch_all(
        self,
        table_id: str,
        fields: Optional[List[str]] = None,
        formula: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Fetch all records from an Airtable table.

        Args:
            table_id: The Airtable table ID or name.
            fields: Optional list of field names to return.
            formula: Optional Airtable formula to filter records.

        Returns:
            List of raw pyairtable record dicts, each with ``"id"`` and
            ``"fields"`` keys.
        """
        table = self._table(table_id)
        kwargs: Dict[str, Any] = {}
        if fields:
            kwargs["fields"] = fields
        if formula:
            kwargs["formula"] = formula
        return table.all(**kwargs)

    # ------------------------------------------------------------------
    # Comments API (via pyairtable)
    # ------------------------------------------------------------------

    def list_comments(self, table_id: str, record_id: str) -> list:
        """List all comments on a record. Returns pyairtable Comment objects.

        Pagination is handled automatically by pyairtable.
        """
        self._rate_limit_sleep()
        table = self._table(table_id)
        return table.comments(record_id)

    def add_comment(
        self,
        table_id: str,
        record_id: str,
        text: str,
        dry_run: bool = False,
    ) -> bool:
        """Add a comment to a record.

        Returns True on success, False on error.
        """
        if dry_run:
            return True

        self._rate_limit_sleep()
        table = self._table(table_id)
        try:
            table.add_comment(record_id, text)
            return True
        except Exception as e:
            print(f"    Comment API error: {e}")
            return False
