# 100GB+ Scalability Architecture

## 1) End-to-End Architecture

1. Data ingestion layer (batch + streaming)
2. Data lakehouse storage (Parquet + Delta Lake)
3. Analytical compute layer (Spark SQL)
4. Retrieval layer (metadata + optional vector index)
5. LLM orchestration layer (multi-agent)
6. Monitoring and evaluation layer

## 2) A. Data Engineering & Preprocessing

### Batch ingestion
- Use `spark/ingest_pyspark.py` to read raw CSV files with explicit schema.
- Apply cleaning rules:
  - trim string keys
  - cast numeric/date types
  - drop malformed rows
  - de-duplicate by business key (`order_id` or transaction ID)
- Write curated output to partitioned Parquet (`txn_date`, `region`, or `country`).

### Streaming ingestion (optional)
- Use Spark Structured Streaming (`spark/streaming_kafka.py`) for Kafka topics.
- Persist micro-batches to bronze/silver Delta tables.

### Recommended bronze/silver/gold pattern
- Bronze: raw append-only events
- Silver: cleaned standardized records
- Gold: business-ready aggregates (daily sales, category KPIs, YoY tables)

## 3) B. Storage & Indexing

### Warehouse
- Parquet for columnar scan efficiency and compression.
- Delta Lake for ACID transactions, schema evolution, and upserts.

### Analytical layer
- Use Delta table registration and Spark SQL for interactive analytics.
- Add partition pruning and Z-order clustering on high-selectivity fields (`txn_date`, `category`, `region`).

### Metadata index
- Persist table stats (row counts, min/max dates, cardinality, null rates).
- Route user questions to relevant tables before SQL generation.

## 4) C. Retrieval & Query Efficiency

### Two-stage retrieval
1. Metadata filtering: select candidate tables/partitions by schema + date + region constraints.
2. Semantic retrieval: ChromaDB index over table descriptions and KPI docs.

### RAG pattern
- Retrieve:
  - schema snippets
  - metric definitions
  - historical summary docs
- Augment LLM prompts with retrieved context to reduce hallucination.

### Query pushdown
- Generate SQL with strong filters (`WHERE date BETWEEN ...`, `region = ...`).
- Keep queries aggregate-first and limit row-level retrieval.

## 5) D. Model Orchestration at Scale

### Multi-agent chain (LangGraph)
- Agent 1: Language-to-query resolution
- Agent 2: Validation (SQL safety + syntax + policy)
- Agent 3: Data extraction
- Agent 4: Response generation/grounding

### Performance controls
- Prompt caching in SQLite/Redis keyed by normalized prompt hash.
- Result caching for repeated business questions.
- Dynamic model routing:
  - smaller model for classification/routing
  - stronger model for final synthesis when needed
- Token budget guardrails and max-row limits.

## 6) E. Monitoring & Evaluation

### Online metrics
- Accuracy proxy: validation pass rate, execution success rate, human correction rate
- Latency: p50/p95 total response latency + DB latency + LLM latency
- Cost: tokens per request, cache hit rate, estimated USD/request

### Quality evaluation
- Build benchmark question set (time-based, region-based, category-based).
- Score exactness of numeric answers and consistency of narrative summary.

### Fallback strategies
- If query validation fails:
  - attempt auto-repair once
  - return safe clarification question
- If confidence is low:
  - return best-effort answer + explicit uncertainty note
  - suggest follow-up filter constraints

## 7) Deployment Blueprint

- Local prototype: SQLite + LangGraph + Mistral API
- Production:
  - Compute: Spark on Kubernetes/EMR/Dataproc
  - Storage: object store (S3/ADLS/GCS) + Delta tables
  - Serving: FastAPI + async workers + queue for heavy jobs
  - Observability: Prometheus + Grafana + structured logs
