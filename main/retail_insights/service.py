from __future__ import annotations

from dataclasses import asdict
from typing import Any, Optional

from .agents import (
    DataExtractionAgent,
    QueryEnhancementAgent,
    QueryResolutionAgent,
    ResponseAgent,
    ValidationAgent,
)
from .cache import SqlitePromptCache
from .config import AssistantConfig
from .graph import RetailInsightsGraph
from .ingestion import DataIngestionService
from .llm import MistralClient
from .memory import ConversationMemory
from .monitoring import MetricsCollector
from .retrieval import MetadataRetriever
from .sqlite_utils import ensure_system_tables


class RetailInsightsAssistant:
    def __init__(self, config: AssistantConfig):
        self.config = config
        ensure_system_tables(config.db_path)

        self.cache = SqlitePromptCache(config.db_path)
        self.memory = ConversationMemory(config.db_path, max_turns=config.memory_turns)
        self.llm = MistralClient(
            model=config.mistral_model,
            api_key=config.mistral_api_key,
            temperature=config.llm_temperature,
            cache=self.cache,
        )
        self.ingestion = DataIngestionService(config.db_path)
        self.retriever = MetadataRetriever(
            db_path=config.db_path,
            chroma_persist_path=config.chroma_persist_path,
            embedding_model_name=config.embedding_model_name,
            llm_client=self.llm,
        )
        self.retriever.refresh()

        enhancement_agent = QueryEnhancementAgent(config.db_path, self.llm)
        query_agent = QueryResolutionAgent(config.db_path, self.llm, self.retriever, self.memory)
        validation_agent = ValidationAgent(config.db_path, self.llm)
        extraction_agent = DataExtractionAgent(config.db_path, max_rows=config.max_sql_rows)
        response_agent = ResponseAgent(self.llm, self.memory)
        self.graph = RetailInsightsGraph(
            enhancement_agent,
            query_agent,
            validation_agent,
            extraction_agent,
            response_agent,
        )

    def ingest(self, input_path: str) -> list[dict[str, Any]]:
        results = self.ingestion.ingest(input_path)
        self.retriever.refresh()
        return results

    def summarize(
        self,
        conversation_id: str = "default",
        prompt: Optional[str] = None,
        debug: bool = False,
    ) -> dict[str, Any]:
        question = prompt or "Summarize the most important business performance insights from the loaded sales data."
        return self._run(question, mode="summary", conversation_id=conversation_id, debug=debug)

    def ask(self, question: str, conversation_id: str = "default", debug: bool = False) -> dict[str, Any]:
        return self._run(question, mode="qa", conversation_id=conversation_id, debug=debug)

    def _run(self, question: str, mode: str, conversation_id: str, debug: bool = False) -> dict[str, Any]:
        metrics = MetricsCollector()
        state: dict[str, Any] = {
            "conversation_id": conversation_id,
            "mode": mode,
            "user_query": question,
            "metrics": {},
            "debug": debug,
            "debug_info": {},
        }

        with metrics.timer("graph_latency_seconds"):
            result = self.graph.invoke(state)

        result_metrics = result.get("metrics", {})
        if not isinstance(result_metrics, dict):
            result_metrics = {}
        result_metrics.update(metrics.values)
        result["metrics"] = result_metrics
        return result

    def config_snapshot(self) -> dict[str, Any]:
        return asdict(self.config)
