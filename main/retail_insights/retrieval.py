from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Iterable, Optional, Tuple

import chromadb
from chromadb.api.types import Documents, EmbeddingFunction, Embeddings
from sentence_transformers import SentenceTransformer

from .prompts import (
    QUESTION_ROUTER_SYSTEM,
    TABLE_DOC_SYSTEM,
    TABLE_PROFILER_SYSTEM,
    TABLE_RERANK_SYSTEM,
    TABLE_RERANK_TEMPLATE,
)
from .sqlite_utils import sqlite_conn


TOKEN_RE = re.compile(r"[a-zA-Z0-9_]+")
EMBEDDING_MODEL_NAME = "Alibaba-NLP/gte-large-en-v1.5"
COLLECTION_NAME = "schema_metadata"
ALLOWED_TABLE_KINDS = {"fact_sales", "dim_inventory", "dim_pricing", "dim_customer", "misc"}
TABLE_KIND_ALIASES = {
    "fact_sales": "fact_sales",
    "sales": "fact_sales",
    "sales_fact": "fact_sales",
    "fact": "fact_sales",
    "factsales": "fact_sales",
    "dim_inventory": "dim_inventory",
    "inventory": "dim_inventory",
    "stock": "dim_inventory",
    "dim_pricing": "dim_pricing",
    "pricing": "dim_pricing",
    "price": "dim_pricing",
    "dim_customer": "dim_customer",
    "customer": "dim_customer",
    "customers": "dim_customer",
    "misc": "misc",
    "other": "misc",
    "unknown": "misc",
    "general": "misc",
}


def _tokenize(text: str) -> list[str]:
    return [token.lower() for token in TOKEN_RE.findall(text)]


@dataclass
class TableDoc:
    table_name: str
    document_text: str
    profile: dict[str, Any]


class GteEmbeddingFunction(EmbeddingFunction):
    def __init__(self, model_name: str):
        self.model = SentenceTransformer(model_name, trust_remote_code=True)

    def __call__(self, input: Documents) -> Embeddings:
        texts = list(input)
        vectors = self.model.encode(texts, normalize_embeddings=True)
        return vectors.tolist()


class MetadataRetriever:
    def __init__(
        self,
        db_path: str,
        chroma_persist_path: str = "./chroma_db",
        embedding_model_name: str = EMBEDDING_MODEL_NAME,
        llm_client: Optional[Any] = None,
    ):
        if llm_client is None:
            raise ValueError("MetadataRetriever requires an LLM client.")
        self.db_path = db_path
        self.chroma_persist_path = chroma_persist_path
        self.embedding_model_name = embedding_model_name
        self.llm_client = llm_client

        self.docs: list[TableDoc] = []
        self.collection = None

        self._chroma_ready = False
        self._init_chroma()

    def _init_chroma(self) -> None:
        client = chromadb.PersistentClient(path=self.chroma_persist_path)
        embedding_fn = GteEmbeddingFunction(self.embedding_model_name)
        self.collection = client.get_or_create_collection(
            name=COLLECTION_NAME,
            embedding_function=embedding_fn,
        )
        self._chroma_ready = True

    def refresh(self) -> None:
        with sqlite_conn(self.db_path) as conn:
            rows = conn.execute(
                "SELECT table_name, row_count, columns_json FROM table_metadata ORDER BY table_name"
            ).fetchall()

        self.docs = []
        ids: list[str] = []
        documents: list[str] = []
        metadatas: list[dict[str, str]] = []

        for row in rows:
            table_name = row["table_name"]
            row_count = int(row["row_count"])
            columns = json.loads(row["columns_json"])
            current_hash = self._schema_hash(table_name, columns)

            cached = self._read_cached_profile(table_name)
            if cached and cached.get("schema_hash") == current_hash:
                profile = json.loads(cached["profile_json"])
                document_text = cached["document_text"]
                if not isinstance(profile, dict):
                    raise ValueError(f"Cached profile for {table_name} is not a JSON object.")
            else:
                sample_rows = self._get_sample_rows(table_name, num_rows=3)
                profile = self._profile_table_with_llm(table_name, row_count, columns, sample_rows)
                document_text = self._build_table_document_with_llm(profile)
                self._write_cached_profile(table_name, current_hash, profile, document_text)

            profile = self._normalize_profile(profile, table_name, row_count, columns)
            if not document_text or not document_text.strip():
                raise ValueError(f"Empty table document generated for {table_name}.")

            self.docs.append(TableDoc(table_name=table_name, document_text=document_text, profile=profile))
            ids.append(f"table_profile_{table_name}")
            documents.append(document_text)
            metadatas.append(self._profile_metadata(profile, current_hash, row_count))

        if not self._chroma_ready or self.collection is None:
            raise RuntimeError("Chroma collection is not initialized.")
        if documents:
            self.collection.upsert(ids=ids, documents=documents, metadatas=metadatas)

    def top_tables(self, question: str, k: int = 3) -> list[str]:
        tables, _ = self.top_tables_with_debug(question, k=k)
        return tables

    def top_tables_with_debug(self, question: str, k: int = 3) -> Tuple[list[str], dict[str, Any]]:
        if not self.docs:
            self.refresh()
        if not self.docs:
            raise RuntimeError("No indexed docs available.")
        if not self._chroma_ready or self.collection is None:
            raise RuntimeError("Chroma collection is not initialized.")

        routing = self._question_requirements(question)
        where: dict[str, Any] = {"type": "table_profile"}

        preferred = routing.get("preferred_table_kind")
        if isinstance(preferred, list) and preferred:
            candidate_kind = str(preferred[0]).strip().lower()
            if candidate_kind in ALLOWED_TABLE_KINDS:
                where = {
                    "$and": [
                        {"type": "table_profile"},
                        {"table_kind": candidate_kind},
                    ]
                }

        results = self.collection.query(
            query_texts=[question],
            n_results=min(max(k, 1), len(self.docs)),
            where=where,
            include=["metadatas", "distances"],
        )
        metadatas = results.get("metadatas", [[]])[0]
        tables = [meta.get("table_name", "") for meta in metadatas if meta and meta.get("table_name")]
        distances = results.get("distances", [[]])[0] or []

        if not tables:
            raise RuntimeError("Chroma returned no candidate tables.")

        reranked_tables, rerank_debug = self._rerank_candidate_tables(question, tables)
        return reranked_tables, {
            "retrieval_method": "chroma",
            "candidate_tables": reranked_tables,
            "pre_rerank_tables": tables,
            "distances": distances,
            "rerank": rerank_debug,
            "router_requirements": routing,
            "where_filter": where,
        }

    def context_for_tables(self, table_names: Iterable[str]) -> str:
        names = set(table_names)
        matches = [doc.document_text for doc in self.docs if doc.table_name in names]
        return "\n".join(matches)

    def _rerank_candidate_tables(self, question: str, candidate_tables: list[str]) -> Tuple[list[str], dict[str, Any]]:
        if len(candidate_tables) <= 1:
            return candidate_tables, {
                "method": "skip",
                "selection_reason": "Single candidate table",
                "table_scenarios": self._default_table_scenarios(candidate_tables),
            }

        prompt = TABLE_RERANK_TEMPLATE.format(
            question=question,
            candidate_profiles=self._candidate_profiles_text(candidate_tables),
        )
        response = self.llm_client.complete_json(TABLE_RERANK_SYSTEM, prompt)

        ranked = response.get("ranked_tables")
        if not isinstance(ranked, list):
            raise ValueError("LLM reranker response missing ranked_tables list.")
        ranked_tables = self._normalize_ranked_tables(candidate_tables, ranked)

        table_scenarios = response.get("table_scenarios")
        if table_scenarios is None:
            table_scenarios = []
        if not isinstance(table_scenarios, list):
            raise ValueError("LLM reranker response has invalid table_scenarios.")

        selection_reason = str(response.get("selection_reason", "")).strip()
        return ranked_tables, {
            "method": "llm",
            "selection_reason": selection_reason,
            "table_scenarios": table_scenarios,
        }

    def _candidate_profiles_text(self, candidate_tables: list[str]) -> str:
        lookup = {doc.table_name: doc for doc in self.docs}
        lines: list[str] = []
        for table in candidate_tables:
            doc = lookup.get(table)
            if not doc:
                continue
            profile = doc.profile or {}
            lines.append(
                "\n".join(
                    [
                        f"Table: {table}",
                        f"- table_kind: {profile.get('table_kind', 'unknown')}",
                        f"- grain: {profile.get('grain', '')}",
                        f"- time_columns: {', '.join(profile.get('time_columns', [])) or 'none'}",
                        f"- measure_columns: {', '.join(profile.get('measure_columns', [])) or 'none'}",
                        f"- dimension_columns: {', '.join(profile.get('dimension_columns', [])) or 'none'}",
                        f"- can_answer: {', '.join(profile.get('can_answer', [])) or 'none'}",
                        f"- recommended_filters: {', '.join(profile.get('recommended_filters', [])) or 'none'}",
                        f"- dynamic_scenario_hints: {', '.join(self._dynamic_scenario_hints(table, profile))}",
                    ]
                )
            )
        return "\n\n".join(lines) if lines else "No candidate profile details available."

    @staticmethod
    def _normalize_ranked_tables(candidate_tables: list[str], ranked: Any) -> list[str]:
        allowed = set(candidate_tables)
        filtered = [str(table) for table in ranked if str(table) in allowed]
        if not filtered:
            raise ValueError("LLM reranker returned no valid table names.")
        for table in candidate_tables:
            if table not in filtered:
                filtered.append(table)
        return filtered

    def _default_table_scenarios(self, candidate_tables: list[str]) -> list[dict[str, Any]]:
        lookup = {doc.table_name: doc for doc in self.docs}
        output: list[dict[str, Any]] = []
        for table in candidate_tables:
            doc = lookup.get(table)
            profile = (doc.profile if doc else {}) or {}
            output.append(
                {
                    "table_name": table,
                    "fit_score": 0.0,
                    "best_fit_scenarios": profile.get("can_answer", [])[:3],
                    "weak_fit_scenarios": ["No reranking required with single candidate table"],
                    "reason": f"table_kind={profile.get('table_kind', 'unknown')}",
                }
            )
        return output

    def _dynamic_scenario_hints(self, table_name: str, profile: dict[str, Any]) -> list[str]:
        hints: list[str] = []
        lower_name = table_name.lower()
        dims = [str(v).lower() for v in profile.get("dimension_columns", [])]
        times = [str(v).lower() for v in profile.get("time_columns", [])]
        measures = [str(v).lower() for v in profile.get("measure_columns", [])]

        has_category = any("category" in value for value in dims)
        has_time = any(any(tok in value for tok in ["date", "month", "year", "quarter"]) for value in times) or (
            any("date" in value for value in dims)
        )
        has_amount = any(any(tok in value for tok in ["amount", "revenue", "gross", "sales"]) for value in measures)
        has_price = any(any(tok in value for tok in ["mrp", "tp", "price", "margin"]) for value in measures)
        has_stock = any("stock" in value for value in measures + dims)

        if has_category and has_time and has_amount:
            hints.append("Best for category + period + revenue analysis")
        if has_price:
            hints.append("Best for pricing and margin-proxy analysis")
        if has_stock:
            hints.append("Best for inventory/stock analysis")
        if "amazon_sale_report" in lower_name:
            hints.append("Strong fit for domestic category/date/amount queries")
        if "international_sale_report" in lower_name:
            hints.append("Strong fit for international date/gross amount queries")
        if not hints:
            hints.append("General-purpose table; validate required fields")
        return hints

    def _profile_table_with_llm(
        self,
        table_name: str,
        row_count: int,
        columns: list[str],
        sample_rows: list[dict[str, Any]],
    ) -> dict[str, Any]:
        payload = json.dumps(
            {
                "table_name": table_name,
                "row_count": row_count,
                "columns": columns,
                "sample_rows": sample_rows,
            },
            ensure_ascii=True,
        )
        response = self.llm_client.complete_json(TABLE_PROFILER_SYSTEM, payload)
        profile = self._normalize_profile(response, table_name, row_count, columns)
        notes = profile.get("notes") or []
        notes.append("profile_source: llm")
        profile["notes"] = notes
        return profile

    def _build_table_document_with_llm(self, profile: dict[str, Any]) -> str:
        payload = json.dumps({"profile": profile}, ensure_ascii=True)
        text = self.llm_client.complete(TABLE_DOC_SYSTEM, payload)
        stripped = text.strip()
        if not stripped:
            raise ValueError("LLM table doc generation returned empty text.")
        return stripped

    def _question_requirements(self, question: str) -> dict[str, Any]:
        payload = json.dumps({"question": question}, ensure_ascii=True)
        raw = self.llm_client.complete_json(QUESTION_ROUTER_SYSTEM, payload)
        return self._normalize_requirements(raw)

    def _normalize_requirements(self, raw: dict[str, Any]) -> dict[str, Any]:
        preferred_raw = raw.get("preferred_table_kind")
        if isinstance(preferred_raw, str):
            preferred_raw = [preferred_raw]
        if not isinstance(preferred_raw, list):
            raise ValueError("Router output must include preferred_table_kind list.")
        preferred: list[str] = []
        for item in preferred_raw:
            raw_kind = str(item).strip().lower().replace("-", "_").replace(" ", "_")
            mapped_kind = TABLE_KIND_ALIASES.get(raw_kind, raw_kind)
            if mapped_kind in ALLOWED_TABLE_KINDS and mapped_kind not in preferred:
                preferred.append(mapped_kind)
        if not preferred:
            raise ValueError(f"Router output has no valid preferred_table_kind: {json.dumps(raw, ensure_ascii=True)}")

        needs = raw.get("needs")
        if not isinstance(needs, dict):
            needs = {}
        measure = needs.get("measure")
        if not isinstance(measure, list):
            measure = []
        dimension = needs.get("dimension")
        if not isinstance(dimension, list):
            dimension = []

        suggested_filters = raw.get("suggested_filters")
        if not isinstance(suggested_filters, list):
            suggested_filters = []
        keywords = raw.get("keywords")
        if not isinstance(keywords, list):
            keywords = _tokenize(json.dumps(raw, ensure_ascii=True))[:20]

        return {
            "preferred_table_kind": preferred,
            "needs": {
                "time": bool(needs.get("time", False)),
                "time_granularity": str(needs.get("time_granularity", "any")),
                "measure": [str(item) for item in measure],
                "dimension": [str(item) for item in dimension],
            },
            "suggested_filters": [str(item) for item in suggested_filters],
            "keywords": [str(item) for item in keywords],
        }

    def _normalize_profile(
        self,
        profile: dict[str, Any],
        table_name: str,
        row_count: int,
        columns: list[str],
    ) -> dict[str, Any]:
        colset = set(columns)
        table_kind = str(profile.get("table_kind", "misc")).strip().lower()
        if table_kind not in ALLOWED_TABLE_KINDS:
            table_kind = "misc"

        time_columns = [col for col in profile.get("time_columns", []) if col in colset]
        measure_columns = [col for col in profile.get("measure_columns", []) if col in colset]
        dimension_columns = [col for col in profile.get("dimension_columns", []) if col in colset]

        join_keys_raw = profile.get("join_keys")
        if not isinstance(join_keys_raw, list):
            join_keys_raw = []
        join_keys: list[dict[str, Any]] = []
        for item in join_keys_raw:
            if not isinstance(item, dict):
                continue
            column = str(item.get("column", "")).strip()
            if column and column in colset:
                maps_to = item.get("maps_to")
                if not isinstance(maps_to, list):
                    maps_to = []
                join_keys.append({"column": column, "maps_to": [str(value) for value in maps_to]})

        can_answer = profile.get("can_answer")
        if not isinstance(can_answer, list):
            can_answer = []
        recommended_filters = profile.get("recommended_filters")
        if not isinstance(recommended_filters, list):
            recommended_filters = []
        notes = profile.get("notes")
        if not isinstance(notes, list):
            notes = []

        return {
            "table_name": table_name,
            "table_kind": table_kind,
            "grain": str(profile.get("grain", "one row per record")),
            "time_columns": [str(col) for col in time_columns],
            "measure_columns": [str(col) for col in measure_columns],
            "dimension_columns": [str(col) for col in dimension_columns],
            "join_keys": join_keys,
            "can_answer": [str(item) for item in can_answer],
            "recommended_filters": [str(item) for item in recommended_filters],
            "notes": [str(item) for item in notes],
            "row_count": row_count,
        }

    def _profile_metadata(self, profile: dict[str, Any], schema_hash: str, row_count: int) -> dict[str, str]:
        return {
            "type": "table_profile",
            "table_name": str(profile.get("table_name", "")),
            "table_kind": str(profile.get("table_kind", "misc")),
            "grain": str(profile.get("grain", "")),
            "time_cols": ",".join(profile.get("time_columns", [])),
            "measure_cols": ",".join(profile.get("measure_columns", [])),
            "dimension_cols": ",".join(profile.get("dimension_columns", [])),
            "join_keys_json": json.dumps(profile.get("join_keys", []), ensure_ascii=True),
            "schema_hash": schema_hash,
            "row_count": str(row_count),
        }

    def _get_sample_rows(self, table_name: str, num_rows: int = 3) -> list[dict[str, Any]]:
        with sqlite_conn(self.db_path) as conn:
            rows = conn.execute(f'SELECT * FROM "{table_name}" LIMIT {num_rows}').fetchall()
        return [dict(row) for row in rows]

    def _read_cached_profile(self, table_name: str) -> Optional[dict[str, str]]:
        with sqlite_conn(self.db_path) as conn:
            row = conn.execute(
                """
                SELECT schema_hash, profile_json, document_text
                FROM table_profile_cache
                WHERE table_name = ?
                """,
                (table_name,),
            ).fetchone()
        if not row:
            return None
        return {
            "schema_hash": str(row["schema_hash"]),
            "profile_json": str(row["profile_json"]),
            "document_text": str(row["document_text"]),
        }

    def _write_cached_profile(
        self,
        table_name: str,
        schema_hash: str,
        profile: dict[str, Any],
        document_text: str,
    ) -> None:
        now = datetime.now(timezone.utc).isoformat()
        with sqlite_conn(self.db_path) as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO table_profile_cache
                (table_name, schema_hash, profile_json, document_text, updated_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                (table_name, schema_hash, json.dumps(profile, ensure_ascii=True), document_text, now),
            )
            conn.commit()

    @staticmethod
    def _schema_hash(table_name: str, columns: list[str]) -> str:
        payload = table_name.lower() + "|" + "|".join(col.lower() for col in columns)
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()
