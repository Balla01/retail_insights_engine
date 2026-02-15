from __future__ import annotations

import json
import re
from typing import Any

from .llm import MistralClient
from .memory import ConversationMemory
from .prompts import (
    ANSWER_SYSTEM,
    ANSWER_TEMPLATE,
    QUERY_ENHANCEMENT_SYSTEM,
    QUERY_ENHANCEMENT_TEMPLATE,
    QUERY_RESOLUTION_SYSTEM,
    QUERY_RESOLUTION_TEMPLATE,
)
from .retrieval import MetadataRetriever
from .sqlite_utils import sqlite_conn


UNSAFE_SQL_PATTERNS = [
    re.compile(r"\b(insert|update|delete|drop|alter|create|attach|detach|pragma)\b", re.I),
]


def _merge_debug(state: dict[str, Any], updates: dict[str, Any]) -> dict[str, Any]:
    merged = dict(state.get("debug_info") or {})
    merged.update(updates)
    return merged


class QueryEnhancementAgent:
    def __init__(self, db_path: str, llm: MistralClient):
        self.db_path = db_path
        self.llm = llm

    def run(self, state: dict[str, Any]) -> dict[str, Any]:
        original_query = (state.get("user_query") or "").strip()
        if not original_query:
            original_query = "Summarize sales performance with key trends."
        debug_enabled = bool(state.get("debug"))

        schema_context = self._schema_snapshot()
        prompt = QUERY_ENHANCEMENT_TEMPLATE.format(
            schema_context=schema_context,
            user_query=original_query,
        )

        enhanced_query = self.llm.complete(QUERY_ENHANCEMENT_SYSTEM, prompt).strip()
        if not enhanced_query:
            raise ValueError("Query enhancement returned empty response.")

        return {
            "enhanced_query": enhanced_query,
            "debug_info": _merge_debug(
                state,
                {
                    "query_enhancement": {
                        "original_query": original_query,
                        "enhanced_query": enhanced_query,
                        "prompt": {
                            "system": QUERY_ENHANCEMENT_SYSTEM,
                            "user": prompt,
                        },
                    }
                },
            )
            if debug_enabled
            else state.get("debug_info"),
        }

    def _schema_snapshot(self, max_tables: int = 8) -> str:
        with sqlite_conn(self.db_path) as conn:
            tables = conn.execute(
                "SELECT table_name, columns_json FROM table_metadata ORDER BY table_name LIMIT ?",
                (max_tables,),
            ).fetchall()
        if not tables:
            return "No schema metadata available."

        lines: list[str] = []
        for row in tables:
            cols = json.loads(row["columns_json"])
            lines.append(f"- {row['table_name']}: {', '.join(cols[:20])}")
        return "\n".join(lines)


class QueryResolutionAgent:
    def __init__(
        self,
        db_path: str,
        llm: MistralClient,
        retriever: MetadataRetriever,
        memory: ConversationMemory,
    ):
        self.db_path = db_path
        self.llm = llm
        self.retriever = retriever
        self.memory = memory

    def run(self, state: dict[str, Any]) -> dict[str, Any]:
        question = (state.get("enhanced_query") or state.get("user_query") or "").strip()
        if not question:
            question = "Give me a concise sales performance summary."
        mode = state.get("mode", "qa")
        conversation_id = state.get("conversation_id", "default")
        debug_enabled = bool(state.get("debug"))

        retrieval_debug: dict[str, Any] = {}
        if debug_enabled:
            candidate_tables, retrieval_debug = self.retriever.top_tables_with_debug(question, k=3)
        else:
            candidate_tables = self.retriever.top_tables(question, k=3)
        conversation_context = self._conversation_context(conversation_id)
        schema_context = self._schema_context(candidate_tables)
        prompt = QUERY_RESOLUTION_TEMPLATE.format(
            # conversation_context=conversation_context,
            conversation_context="",
            schema_context=schema_context,
            mode=mode,
            question=question,
        )

        response = self.llm.complete_json(QUERY_RESOLUTION_SYSTEM, prompt)
        sql_query = str(response.get("sql_query", "")).strip()
        confidence = float(response.get("confidence", 0.5))
        notes = str(response.get("notes", ""))
        resolved_mode = response.get("mode", mode)
        if resolved_mode not in {"summary", "qa"}:
            resolved_mode = mode
        tables = response.get("candidate_tables") or candidate_tables
        if not sql_query:
            raise ValueError("Empty SQL from LLM")
        if not tables:
            raise ValueError("No candidate tables selected by resolver.")

        return {
            "schema_context": schema_context,
            "candidate_tables": tables,
            "proposed_sql": sql_query,
            "confidence": confidence,
            "sql_validation_notes": notes,
            "mode": resolved_mode,
            "debug_info": _merge_debug(
                state,
                {
                    "retrieval": retrieval_debug if debug_enabled else {"candidate_tables": tables},
                    "query_sql_prompt": {
                        "system": QUERY_RESOLUTION_SYSTEM,
                        "user": prompt,
                    },
                },
            )
            if debug_enabled
            else state.get("debug_info"),
        }

    def _conversation_context(self, conversation_id: str) -> str:
        history = self.memory.recent_history(conversation_id)
        if not history:
            return "No prior context."
        return "\n".join(f"{role}: {content}" for role, content in history)

    def _schema_context(self, tables: list[str]) -> str:
        context_lines: list[str] = []
        with sqlite_conn(self.db_path) as conn:
            if not tables:
                rows = conn.execute("SELECT table_name FROM table_metadata ORDER BY table_name").fetchall()
                tables = [row["table_name"] for row in rows]

            for table in tables:
                cols = conn.execute(f"PRAGMA table_info({table})").fetchall()
                if not cols:
                    continue

                # Pull a tiny sample once and derive per-column sample values from it.
                sample_rows = conn.execute(f'SELECT * FROM "{table}" LIMIT 3').fetchall()
                sample_map: dict[str, list[str]] = {row["name"]: [] for row in cols}

                for sample_row in sample_rows:
                    for col in cols:
                        col_name = row_name = col["name"]
                        value = sample_row[row_name]
                        if value is None:
                            continue
                        as_text = str(value).strip()
                        if not as_text:
                            continue
                        if as_text not in sample_map[col_name]:
                            sample_map[col_name].append(as_text)

                column_lines: list[str] = []
                for col in cols:
                    col_name = col["name"]
                    col_type = col["type"] or "TEXT"
                    samples = sample_map.get(col_name, [])[:3]
                    pretty_samples = ", ".join(repr(v[:40] + ("..." if len(v) > 40 else "")) for v in samples)
                    if not pretty_samples:
                        pretty_samples = "none"
                    column_lines.append(f"  - {col_name} ({col_type}) | samples: {pretty_samples}")

                context_lines.append(f"Table: {table}\n" + "\n".join(column_lines))

        return "\n\n".join(context_lines) if context_lines else "No table metadata available."

class ValidationAgent:
    def __init__(self, db_path: str, llm: MistralClient):
        self.db_path = db_path
        self.llm = llm

    def run(self, state: dict[str, Any]) -> dict[str, Any]:
        sql = (state.get("proposed_sql") or "").strip()
        debug_enabled = bool(state.get("debug"))

        normalized_sql, note = self._validate_sql(sql)
        return {
            "validated_sql": normalized_sql,
            "sql_valid": True,
            "sql_validation_notes": note,
            "debug_info": _merge_debug(
                state,
                {
                    "sql_validation": {
                        "proposed_sql": sql,
                        "validated_sql": normalized_sql,
                        "sql_valid": True,
                        "notes": note,
                    }
                },
            )
            if debug_enabled
            else state.get("debug_info"),
        }

    def _validate_sql(self, sql: str) -> tuple[str, str]:
        if not sql:
            raise ValueError("SQL is empty.")

        normalized = self._normalize_sql(sql)
        if not normalized.lower().startswith(("select", "with")):
            raise ValueError("Only SELECT/CTE queries are allowed.")

        for pattern in UNSAFE_SQL_PATTERNS:
            if pattern.search(normalized):
                raise ValueError("Unsafe SQL keyword detected.")

        with sqlite_conn(self.db_path) as conn:
            conn.execute(f"EXPLAIN QUERY PLAN {normalized}")

        return normalized, "SQL is valid"

    @staticmethod
    def _normalize_sql(sql: str) -> str:
        cleaned = sql.strip().strip("`")
        if cleaned.endswith(";"):
            return cleaned
        return f"{cleaned};"


class DataExtractionAgent:
    def __init__(self, db_path: str, max_rows: int):
        self.db_path = db_path
        self.max_rows = max_rows

    def run(self, state: dict[str, Any]) -> dict[str, Any]:
        sql = state.get("validated_sql")
        if not sql:
            return {"query_columns": [], "query_rows": [], "result_row_count": 0}
        debug_enabled = bool(state.get("debug"))

        with sqlite_conn(self.db_path) as conn:
            cursor = conn.execute(sql)
            col_names = [desc[0] for desc in (cursor.description or [])]
            rows = cursor.fetchmany(self.max_rows)

        row_dicts = [dict(zip(col_names, row)) for row in rows]
        raw_rows = [list(row) for row in rows]
        return {
            "query_columns": col_names,
            "query_rows": row_dicts,
            "result_row_count": len(row_dicts),
            "debug_info": _merge_debug(
                state,
                {
                    "sql_raw_output": {
                        "executed_sql": sql,
                        "columns": col_names,
                        "rows": raw_rows,
                        "row_count": len(raw_rows),
                    }
                },
            )
            if debug_enabled
            else state.get("debug_info"),
        }


class ResponseAgent:
    def __init__(self, llm: MistralClient, memory: ConversationMemory):
        self.llm = llm
        self.memory = memory

    def run(self, state: dict[str, Any]) -> dict[str, Any]:
        error = state.get("error")
        if error:
            raise RuntimeError(str(error))
        debug_enabled = bool(state.get("debug"))

        conversation_id = state.get("conversation_id", "default")
        question = state.get("user_query", "")
        mode = state.get("mode", "qa")
        sql_query = state.get("validated_sql", "")
        columns = state.get("query_columns", [])
        rows = state.get("query_rows", [])
        row_count = state.get("result_row_count", 0)

        history = self.memory.recent_history(conversation_id)
        conversation_context = "\n".join(f"{role}: {content}" for role, content in history) or "No prior context."

        prompt = ANSWER_TEMPLATE.format(
            mode=mode,
            question=question,
            sql_query=sql_query,
            columns=columns,
            rows=rows[:30],
            row_count=row_count,
            # conversation_context=conversation_context,
            conversation_context="",
        )

        answer = self.llm.complete(ANSWER_SYSTEM, prompt).strip()
        if not answer:
            raise ValueError("Final answer generation returned empty text.")

        confidence = float(state.get("confidence", 0.5))
        answer = f"{answer}\n\nExecution confidence: {confidence:.2f}"

        self.memory.add_turn(conversation_id, "user", question)
        self.memory.add_turn(conversation_id, "assistant", answer)

        return {
            "final_answer": answer,
            "debug_info": _merge_debug(
                state,
                {
                    "final_answer_prompt": {
                        "system": ANSWER_SYSTEM,
                        "user": prompt,
                    }
                },
            )
            if debug_enabled
            else state.get("debug_info"),
        }
