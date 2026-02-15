"""Backward-compatible quick inference script using the new Retail Insights Assistant."""

from __future__ import annotations

import argparse

from retail_insights.config import AssistantConfig
from retail_insights.service import RetailInsightsAssistant


def main() -> None:
    parser = argparse.ArgumentParser(description="Quick natural language query over SQLite retail DB")
    parser.add_argument("--db-path", default="./retail_insights.db")
    parser.add_argument("--question", required=True)
    parser.add_argument("--conversation-id", default="infer")
    args = parser.parse_args()

    assistant = RetailInsightsAssistant(AssistantConfig(db_path=args.db_path))
    result = assistant.ask(args.question, conversation_id=args.conversation_id)
    print(result.get("final_answer", "No answer."))


if __name__ == "__main__":
    main()
