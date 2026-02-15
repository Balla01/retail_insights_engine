from __future__ import annotations

import argparse
import json

from retail_insights.config import AssistantConfig
from retail_insights.service import RetailInsightsAssistant


def print_debug_info(result: dict) -> None:
    debug_info = result.get("debug_info") or {}
    if not debug_info:
        print("\n[DEBUG] No debug info available.")
        return
    print("\n[DEBUG] " + json.dumps(debug_info, indent=2, ensure_ascii=True))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Retail Insights Assistant")
    parser.add_argument("--db-path", default="./retail_insights.db", help="SQLite database path")
    parser.add_argument("--model", default="mistral-medium-latest", help="Mistral model name")

    subparsers = parser.add_subparsers(dest="command", required=True)

    ingest = subparsers.add_parser("ingest", help="Ingest CSV/Excel/JSON/text input")
    ingest.add_argument("--input-path", required=True, help="File or folder path")

    summarize = subparsers.add_parser("summarize", help="Generate summary insights")
    summarize.add_argument("--conversation-id", default="default")
    summarize.add_argument("--prompt", default=None)
    summarize.add_argument("--debug", action="store_true")

    ask = subparsers.add_parser("ask", help="Ask ad-hoc business question")
    ask.add_argument("--conversation-id", default="default")
    ask.add_argument("--question", required=True)
    ask.add_argument("--debug", action="store_true")

    interactive = subparsers.add_parser("interactive", help="Interactive Q&A shell")
    interactive.add_argument("--conversation-id", default="default")
    interactive.add_argument("--debug", action="store_true")

    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    config = AssistantConfig(db_path=args.db_path, mistral_model=args.model)
    assistant = RetailInsightsAssistant(config)

    if args.command == "ingest":
        results = assistant.ingest(args.input_path)
        print(json.dumps({"ingested": results}, indent=2))
        return

    if args.command == "summarize":
        result = assistant.summarize(
            conversation_id=args.conversation_id,
            prompt=args.prompt,
            debug=args.debug,
        )
        print(result.get("final_answer", "No answer."))
        print(json.dumps({"metrics": result.get("metrics", {})}, indent=2))
        if args.debug:
            print_debug_info(result)
        return

    if args.command == "ask":
        result = assistant.ask(args.question, conversation_id=args.conversation_id, debug=args.debug)
        print(result.get("final_answer", "No answer."))
        print(json.dumps({"metrics": result.get("metrics", {})}, indent=2))
        if args.debug:
            print_debug_info(result)
        return

    if args.command == "interactive":
        print("Retail Insights interactive mode. Type 'exit' to quit.")
        while True:
            question = input("\nAsk> ").strip()
            if question.lower() in {"exit", "quit"}:
                break
            result = assistant.ask(question, conversation_id=args.conversation_id, debug=args.debug)
            print(result.get("final_answer", "No answer."))
            if args.debug:
                print_debug_info(result)
        return


if __name__ == "__main__":
    main()
