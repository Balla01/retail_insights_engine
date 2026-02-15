# Retail Insights Assistant (GenAI + Scalable Data System)

This project implements a multi-agent Retail Insights Assistant that:
- Ingests sales input files (`.csv`, `.xlsx`, `.json`, `.txt`) into SQLite
- Supports **Summarization Mode** and **Conversational Q&A Mode**
- Uses a **multi-agent pipeline** (LangGraph) with required agents:
  - Language-to-query resolution agent
  - Data extraction agent
  - Validation agent
- Integrates with **Mistral** for SQL planning and business-language responses
- Adds semantic schema retrieval with **ChromaDB** (fallback to keyword retrieval)
- Provides architecture/scripts for scaling to **100GB+** using PySpark + Delta Lake

## Project Structure

- `main.py`: CLI entrypoint
- `gradio_app.py`: optional UI
- `retail_insights/`
  - `ingestion.py`: file ingestion + schema normalization + SQLite load
  - `agents.py`: multi-agent logic
  - `graph.py`: LangGraph orchestration
  - `service.py`: application service API
  - `llm.py`: Mistral wrapper + response caching
  - `memory.py`: conversation memory store
  - `retrieval.py`: metadata/semantic table retrieval (ChromaDB + `Alibaba-NLP/gte-large-en-v1.5`)
- `spark/`
  - `ingest_pyspark.py`: batch ingestion + cleaning for 100GB+
  - `lakehouse_delta.py`: Delta Lake warehouse + Spark SQL analytics layer
  - `streaming_kafka.py`: optional streaming ingestion skeleton
- `docs/ARCHITECTURE_100GB.md`: scalability design

## Setup

```bash
cd /home/ntlpt19/personal_projects/sales_rag/main
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Set env vars (recommended):

```bash
export MISTRAL_API_KEY="<your_key>"
export MISTRAL_MODEL="mistral-medium-latest"
```

If `MISTRAL_API_KEY` is missing, the app runs with deterministic fallback responses.

## Run with Your Dataset

Your sample dataset folder:
`/home/ntlpt19/personal_projects/sales_rag/Sales Dataset/Sales Dataset`

### 1) Ingest

```bash
python3 main.py --db-path ./retail_insights.db ingest \
  --input-path "/home/ntlpt19/personal_projects/sales_rag/Sales Dataset/Sales Dataset"
```

### 2) Summarize

```bash
python3 main.py --db-path ./retail_insights.db summarize \
  --conversation-id demo
```

### 3) Ask Ad-hoc Questions

```bash
python3 main.py --db-path ./retail_insights.db ask \
  --conversation-id demo \
  --question "Which category has the highest total amount?"
```

### 4) Interactive Mode

```bash
python3 main.py --db-path ./retail_insights.db interactive --conversation-id demo
```

### 5) Optional Gradio UI

```bash
python3 gradio_app.py
```

## Prompt Engineering + Context

- Prompt templates enforce:
  - JSON-structured query plans
  - read-only SQL policy
  - grounded final responses
- Conversation memory is persisted in SQLite (`conversation_turns`)
- LLM outputs are cached in SQLite (`llm_cache`) for latency/cost control

## Notes

- SQLite is the local analytical layer for prototype and moderate-sized workloads.
- For 100GB+ historical data, use Spark + Delta architecture in `docs/ARCHITECTURE_100GB.md` and `spark/` scripts.
