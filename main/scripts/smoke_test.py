from __future__ import annotations

from retail_insights.config import AssistantConfig
from retail_insights.service import RetailInsightsAssistant


def main() -> None:
    dataset_path = "/home/ntlpt19/personal_projects/sales_rag/Sales Dataset/Sales Dataset"
    config = AssistantConfig(db_path="./retail_insights_smoke.db")
    assistant = RetailInsightsAssistant(config)

    print("Ingesting sample dataset...")
    ingested = assistant.ingest(dataset_path)
    print(f"Loaded {len(ingested)} files")

    print("Running summary...")
    summary = assistant.summarize(conversation_id="smoke")
    print(summary.get("final_answer", ""))

    print("Running QA...")
    answer = assistant.ask(
        "Which category has highest aggregate metric in the most relevant table?",
        conversation_id="smoke",
    )
    print(answer.get("final_answer", ""))


if __name__ == "__main__":
    main()
