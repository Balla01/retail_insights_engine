from __future__ import annotations

from typing import Any

from .models import AgentState
from langgraph.graph import END, StateGraph


class RetailInsightsGraph:
    def __init__(self, enhancement_agent, query_agent, validation_agent, extraction_agent, response_agent):
        self.enhancement_agent = enhancement_agent
        self.query_agent = query_agent
        self.validation_agent = validation_agent
        self.extraction_agent = extraction_agent
        self.response_agent = response_agent
        self._compiled = self._build_graph()

    def _build_graph(self):
        graph = StateGraph(AgentState)
        graph.add_node("query_enhancement", self.enhancement_agent.run)
        graph.add_node("query_resolution", self.query_agent.run)
        graph.add_node("validation", self.validation_agent.run)
        graph.add_node("data_extraction", self.extraction_agent.run)
        graph.add_node("response", self.response_agent.run)

        graph.set_entry_point("query_enhancement")
        graph.add_edge("query_enhancement", "query_resolution")
        graph.add_edge("query_resolution", "validation")
        graph.add_conditional_edges(
            "validation",
            self._route_after_validation,
            {
                "ok": "data_extraction",
                "error": "response",
            },
        )
        graph.add_edge("data_extraction", "response")
        graph.add_edge("response", END)
        return graph.compile()

    @staticmethod
    def _route_after_validation(state: dict[str, Any]) -> str:
        return "ok" if state.get("sql_valid") else "error"

    def invoke(self, state: dict[str, Any]) -> dict[str, Any]:
        return self._compiled.invoke(state)
