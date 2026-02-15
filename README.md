# Retail Insights Assistant

GenAI-powered retail analytics assistant for:

- Dataset ingestion from CSV, Excel, JSON, and TXT
- Natural-language Q&A over sales data
- Automated summary generation
- Multi-agent orchestration (LangGraph)
- Retrieval-augmented table selection (ChromaDB + embeddings)

---

## 1. System Overview

This project converts business questions into SQL, executes SQL on SQLite, and returns grounded business insights.

Core modes:

- `ingest`: Load files into SQLite and refresh retrieval index
- `ask`: Ad-hoc business Q&A
- `summarize`: Concise performance summary
- `interactive`: CLI chat loop
- `gradio_app.py`: Web UI for ingestion, summary, and Q&A

---

## 2. System Architecture

### Flowchart

```mermaid
flowchart TD
    A[User Request: CLI or Gradio] --> B{Mode}

    B -->|ingest| C[DataIngestionService.ingest]
    C --> C1[Read files: CSV/XLSX/JSON/TXT]
    C1 --> C2[Normalize columns/values]
    C2 --> C3[Write business tables to SQLite]
    C3 --> C4[Upsert table_metadata]
    C4 --> C5[MetadataRetriever.refresh]
    C5 --> C6[LLM profile per table]
    C6 --> C7[Create table docs]
    C7 --> C8[Upsert docs + metadata into ChromaDB]
    C8 --> Z[Ingestion Complete]

    B -->|ask/summarize| D[LangGraph Pipeline]
    D --> E[QueryEnhancementAgent]
    E --> F[QueryResolutionAgent]
    F --> F1[Retrieve candidate tables from Chroma]
    F1 --> F2[LLM rerank candidate tables]
    F2 --> F3[Generate SQL from schema context]
    F3 --> G[ValidationAgent]
    G -->|valid| H[DataExtractionAgent]
    G -->|invalid| X[Stop: raise error]
    H --> I[ResponseAgent]
    I --> Y[Final answer + confidence + debug info]
```

### Sequence Diagram

```mermaid
sequenceDiagram
    participant U as User
    participant APP as Assistant Service
    participant GE as QueryEnhancementAgent
    participant RET as Retriever (Chroma + Router + Reranker)
    participant QR as QueryResolutionAgent
    participant VA as ValidationAgent
    participant DB as SQLite
    participant RA as ResponseAgent
    participant LLM as Mistral

    U->>APP: ask(question)
    APP->>GE: enhance query
    GE->>LLM: enhancement prompt
    LLM-->>GE: enhanced question

    APP->>RET: top candidate tables
    RET->>LLM: question router JSON
    RET->>RET: Chroma semantic query
    RET->>LLM: rerank candidates
    RET-->>APP: ranked tables

    APP->>QR: generate SQL
    QR->>LLM: SQL generation prompt
    LLM-->>QR: sql_query JSON

    APP->>VA: validate SQL
    VA->>DB: EXPLAIN QUERY PLAN
    DB-->>VA: validation result

    APP->>DB: execute SQL
    DB-->>APP: rows + columns

    APP->>RA: generate final answer
    RA->>LLM: answer synthesis prompt
    LLM-->>RA: final response
    RA-->>U: answer + confidence
```

---

## 3. Project Structure

```text
retail_insights_engine/
|-- README.md
`-- main/
    |-- main.py
    |-- gradio_app.py
    |-- requirements.txt
    |-- retail_insights/
    |   |-- agents.py
    |   |-- cache.py
    |   |-- config.py
    |   |-- graph.py
    |   |-- ingestion.py
    |   |-- llm.py
    |   |-- memory.py
    |   |-- monitoring.py
    |   |-- prompts.py
    |   |-- retrieval.py
    |   `-- sqlite_utils.py
    `-- spark/
        |-- ingest_pyspark.py
        `-- lakehouse_delta.py
```

---

## 4. Setup and Execution Guide

### 4.1 Clone

```bash
git clone <your-repo-url>
cd retail_insights_engine/main
```

### 4.2 Create environment

```bash
python3 -m venv .venv
source .venv/bin/activate
```

### 4.3 Install dependencies

```bash
pip install -r requirements.txt
```

### 4.4 Configure `.env`

```env
MISTRAL_API_KEY=your_api_key_here
MISTRAL_MODEL=mistral-medium-latest
SQLITE_DB_PATH=./retail_insights.db
CHROMA_PERSIST_PATH=./chroma_db
EMBEDDING_MODEL_NAME=Alibaba-NLP/gte-large-en-v1.5
LLM_TEMPERATURE=0.2
MAX_SQL_ROWS=200
MEMORY_TURNS=8
```

### 4.5 Ingest data

```bash
python3 main.py --db-path ./retail_insights.db ingest --input-path "/path/to/data"
```

### 4.6 Ask a question

```bash
python3 main.py --db-path ./retail_insights.db ask --conversation-id demo --question "Which product line underperformed in Q4?" --debug
```

### 4.7 Summarize

```bash
python3 main.py --db-path ./retail_insights.db summarize --conversation-id demo --debug
```

### 4.8 Interactive CLI

```bash
python3 main.py --db-path ./retail_insights.db interactive --conversation-id demo --debug
```

### 4.9 Run Gradio UI

```bash
python3 gradio_app.py
```

Then open the local URL printed in terminal.

---

## 5. Assumptions, Limitations, and Possible Improvements

### Assumptions

- Input datasets are mostly tabular and can be loaded as flat tables.
- Mistral API access is available and correctly configured.
- Date columns in major sales tables commonly use `MM-DD-YY` text format unless schema suggests otherwise.
- Table-level retrieval is sufficient for selecting relevant sources before SQL generation.

### Limitations

- SQLite is good for local/medium workloads, but not ideal as primary engine for very large interactive analytics.
- SQL and answer quality still depends on LLM output quality and schema clarity.
- Current pipeline is strict fail-fast: malformed LLM JSON/SQL stops execution.
- Gradio UI is minimal and does not include full admin/monitoring controls.

### Possible Improvements

- Add optional guarded fallback mode via config for non-critical environments.
- Add SQL regression tests and benchmark suite.
- Add evaluation dashboard for retrieval precision, SQL validity rate, and latency.
- Integrate Spark/Delta as primary engine for 100GB+ workloads; keep SQLite as serving cache.
- Add enterprise controls: auth, RBAC, audit logs, and rate limits.

---

## 6. Troubleshooting

- `MISTRAL_API_KEY is required`
  - Set API key in `.env`.
- JSON parse or strict-output failures
  - Run with `--debug` and inspect prompt/response traces.
- Empty retrieval candidates
  - Re-run ingestion and ensure retriever index refresh completes.
- Slow first run
  - Embedding model may download on first use.
