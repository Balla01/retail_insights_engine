from __future__ import annotations

from typing import Any, Literal, TypedDict


ModeType = Literal["summary", "qa"]


class AgentState(TypedDict, total=False):
    conversation_id: str
    debug: bool
    mode: ModeType
    user_query: str
    enhanced_query: str
    schema_context: str
    candidate_tables: list[str]
    proposed_sql: str
    validated_sql: str
    sql_valid: bool
    sql_validation_notes: str
    query_columns: list[str]
    query_rows: list[dict[str, Any]]
    result_row_count: int
    draft_answer: str
    final_answer: str
    confidence: float
    error: str
    metrics: dict[str, float]
    debug_info: dict[str, Any]
