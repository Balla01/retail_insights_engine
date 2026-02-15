from __future__ import annotations

import hashlib
from datetime import datetime, timezone
from typing import Optional

from .sqlite_utils import sqlite_conn


class SqlitePromptCache:
    def __init__(self, db_path: str):
        self.db_path = db_path

    @staticmethod
    def _hash(payload: str) -> str:
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()

    def get(self, payload: str) -> Optional[str]:
        key = self._hash(payload)
        with sqlite_conn(self.db_path) as conn:
            row = conn.execute(
                "SELECT response_text FROM llm_cache WHERE cache_key = ?", (key,)
            ).fetchone()
        return row["response_text"] if row else None

    def put(self, payload: str, response_text: str) -> None:
        key = self._hash(payload)
        now = datetime.now(timezone.utc).isoformat()
        with sqlite_conn(self.db_path) as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO llm_cache (cache_key, response_text, created_at)
                VALUES (?, ?, ?)
                """,
                (key, response_text, now),
            )
            conn.commit()
