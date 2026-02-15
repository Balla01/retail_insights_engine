from __future__ import annotations

import gradio as gr

from retail_insights.config import AssistantConfig
from retail_insights.service import RetailInsightsAssistant


config = AssistantConfig()
assistant = RetailInsightsAssistant(config)


def ingest_data(input_path: str) -> str:
    results = assistant.ingest(input_path)
    if not results:
        return "No supported files found."
    lines = [f"{row['table_name']} ({row['row_count']} rows)" for row in results]
    return "Ingested tables:\n" + "\n".join(lines)


def summarize(conversation_id: str, prompt: str) -> str:
    result = assistant.summarize(conversation_id=conversation_id, prompt=prompt or None)
    return result.get("final_answer", "No answer.")


def ask_question(conversation_id: str, question: str) -> str:
    result = assistant.ask(question=question, conversation_id=conversation_id)
    return result.get("final_answer", "No answer.")


with gr.Blocks(title="Retail Insights Assistant") as demo:
    gr.Markdown("# Retail Insights Assistant")
    with gr.Tab("Ingestion"):
        input_path = gr.Textbox(label="Input file/folder path")
        ingest_btn = gr.Button("Ingest")
        ingest_output = gr.Textbox(label="Ingestion Status", lines=8)
        ingest_btn.click(ingest_data, inputs=input_path, outputs=ingest_output)

    with gr.Tab("Summarization"):
        summary_conversation = gr.Textbox(label="Conversation ID", value="default")
        summary_prompt = gr.Textbox(label="Prompt (optional)")
        summary_btn = gr.Button("Generate Summary")
        summary_output = gr.Textbox(label="Summary", lines=10)
        summary_btn.click(
            summarize,
            inputs=[summary_conversation, summary_prompt],
            outputs=summary_output,
        )

    with gr.Tab("Q&A"):
        qa_conversation = gr.Textbox(label="Conversation ID", value="default")
        qa_question = gr.Textbox(label="Question")
        qa_btn = gr.Button("Ask")
        qa_output = gr.Textbox(label="Answer", lines=10)
        qa_btn.click(ask_question, inputs=[qa_conversation, qa_question], outputs=qa_output)


if __name__ == "__main__":
    demo.launch()
