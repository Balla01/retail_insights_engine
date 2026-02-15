from __future__ import annotations

from datetime import datetime, timezone

from .sqlite_utils import sqlite_conn


class ConversationMemory:
    def __init__(self, db_path: str, max_turns: int = 8):
        self.db_path = db_path
        self.max_turns = max_turns

    def add_turn(self, conversation_id: str, role: str, content: str) -> None:
        now = datetime.now(timezone.utc).isoformat()
        with sqlite_conn(self.db_path) as conn:
            conn.execute(
                """
                INSERT INTO conversation_turns (conversation_id, role, content, created_at)
                VALUES (?, ?, ?, ?)
                """,
                (conversation_id, role, content, now),
            )
            conn.commit()

    def recent_history(self, conversation_id: str) -> list[tuple[str, str]]:
        with sqlite_conn(self.db_path) as conn:
            rows = conn.execute(
                """
                SELECT role, content
                FROM conversation_turns
                WHERE conversation_id = ?
                ORDER BY id DESC
                LIMIT ?
                """,
                (conversation_id, self.max_turns),
            ).fetchall()
        rows = list(rows)[::-1]
        return [(row["role"], row["content"]) for row in rows]
