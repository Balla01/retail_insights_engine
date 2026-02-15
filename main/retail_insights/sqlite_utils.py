from __future__ import annotations

import re
import sqlite3
from contextlib import contextmanager
from pathlib import Path


SAFE_IDENTIFIER_RE = re.compile(r"[^a-zA-Z0-9_]+")


def normalize_identifier(name: str) -> str:
    normalized = SAFE_IDENTIFIER_RE.sub("_", name.strip().lower()).strip("_")
    if not normalized:
        normalized = "table"
    if normalized[0].isdigit():
        normalized = f"t_{normalized}"
    return normalized


@contextmanager
def sqlite_conn(db_path: str):
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    try:
        conn.row_factory = sqlite3.Row
        # Avoid filesystem temp writes in constrained environments.
        conn.execute("PRAGMA temp_store = MEMORY;")
        yield conn
    finally:
        conn.close()


def ensure_system_tables(db_path: str) -> None:
    with sqlite_conn(db_path) as conn:
        cursor = conn.cursor()
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS table_metadata (
                table_name TEXT PRIMARY KEY,
                source_path TEXT NOT NULL,
                file_type TEXT NOT NULL,
                row_count INTEGER NOT NULL,
                columns_json TEXT NOT NULL,
                loaded_at TEXT NOT NULL
            )
            """
        )
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS conversation_turns (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                conversation_id TEXT NOT NULL,
                role TEXT NOT NULL,
                content TEXT NOT NULL,
                created_at TEXT NOT NULL
            )
            """
        )
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS llm_cache (
                cache_key TEXT PRIMARY KEY,
                response_text TEXT NOT NULL,
                created_at TEXT NOT NULL
            )
            """
        )
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS table_profile_cache (
                table_name TEXT PRIMARY KEY,
                schema_hash TEXT NOT NULL,
                profile_json TEXT NOT NULL,
                document_text TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )
        conn.commit()
