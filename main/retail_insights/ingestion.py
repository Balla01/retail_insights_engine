from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from .sqlite_utils import ensure_system_tables, normalize_identifier, sqlite_conn

SUPPORTED_EXTENSIONS = {".csv", ".xlsx", ".xls", ".json", ".txt"}


class DataIngestionService:
    def __init__(self, db_path: str):
        self.db_path = db_path
        ensure_system_tables(db_path)

    def ingest(self, path: str) -> list[dict[str, Any]]:
        input_path = Path(path)
        if not input_path.exists():
            raise FileNotFoundError(f"Input path does not exist: {path}")

        files = [input_path] if input_path.is_file() else sorted(
            p for p in input_path.rglob("*") if p.is_file() and p.suffix.lower() in SUPPORTED_EXTENSIONS
        )

        results: list[dict[str, Any]] = []
        for file_path in files:
            suffix = file_path.suffix.lower()
            if suffix == ".csv":
                table_name, row_count, columns = self._ingest_dataframe(file_path, pd.read_csv(file_path))
            elif suffix in {".xlsx", ".xls"}:
                table_name, row_count, columns = self._ingest_dataframe(file_path, pd.read_excel(file_path))
            elif suffix == ".json":
                table_name, row_count, columns = self._ingest_json(file_path)
            elif suffix == ".txt":
                table_name, row_count, columns = self._ingest_text(file_path)
            else:
                continue

            self._upsert_metadata(
                table_name=table_name,
                source_path=str(file_path),
                file_type=suffix.lstrip("."),
                row_count=row_count,
                columns=columns,
            )
            results.append(
                {
                    "table_name": table_name,
                    "source_path": str(file_path),
                    "row_count": row_count,
                    "columns": columns,
                }
            )
        return results

    def schema_context(self) -> str:
        with sqlite_conn(self.db_path) as conn:
            rows = conn.execute(
                "SELECT table_name, row_count, columns_json FROM table_metadata ORDER BY table_name"
            ).fetchall()
        if not rows:
            return "No data is loaded."

        lines: list[str] = []
        for row in rows:
            cols = json.loads(row["columns_json"])
            lines.append(
                f"Table: {row['table_name']} (rows={row['row_count']})\n"
                + "\n".join(f"  - {col}" for col in cols)
            )
        return "\n\n".join(lines)

    def table_names(self) -> list[str]:
        with sqlite_conn(self.db_path) as conn:
            rows = conn.execute("SELECT table_name FROM table_metadata ORDER BY table_name").fetchall()
        return [row["table_name"] for row in rows]

    def _ingest_dataframe(self, file_path: Path, frame: pd.DataFrame) -> tuple[str, int, list[str]]:
        df = frame.copy()
        df.columns = self._normalize_columns(list(df.columns))
        df = self._normalize_values(df)

        table_name = normalize_identifier(file_path.stem)
        with sqlite_conn(self.db_path) as conn:
            df.to_sql(table_name, conn, if_exists="replace", index=False)
            conn.commit()

        return table_name, int(len(df)), list(df.columns)

    def _ingest_json(self, file_path: Path) -> tuple[str, int, list[str]]:
        with file_path.open("r", encoding="utf-8") as handle:
            payload = json.load(handle)

        table_name = normalize_identifier(file_path.stem)
        if isinstance(payload, list):
            df = pd.DataFrame(payload)
            return self._ingest_dataframe(file_path, df)

        # Object JSON payloads are preserved as key/value rows.
        records = [{"json_key": key, "json_value": json.dumps(value)} for key, value in payload.items()]
        df = pd.DataFrame(records)
        return self._ingest_dataframe(file_path, df)

    def _ingest_text(self, file_path: Path) -> tuple[str, int, list[str]]:
        text = file_path.read_text(encoding="utf-8", errors="ignore")
        chunks = [text[i : i + 1200] for i in range(0, len(text), 1200)] or [""]
        df = pd.DataFrame({"chunk_id": list(range(len(chunks))), "content": chunks})
        return self._ingest_dataframe(file_path, df)

    @staticmethod
    def _normalize_columns(columns: list[str]) -> list[str]:
        seen: dict[str, int] = {}
        normalized: list[str] = []
        for col in columns:
            candidate = normalize_identifier(str(col))
            seen[candidate] = seen.get(candidate, 0) + 1
            if seen[candidate] > 1:
                candidate = f"{candidate}_{seen[candidate]}"
            normalized.append(candidate)
        return normalized

    @staticmethod
    def _normalize_values(df: pd.DataFrame) -> pd.DataFrame:
        for col in df.columns:
            if df[col].dtype == object:
                df[col] = df[col].astype(str).str.strip()
                df[col] = df[col].replace({"": None, "nan": None, "None": None})
        return df

    def _upsert_metadata(
        self,
        table_name: str,
        source_path: str,
        file_type: str,
        row_count: int,
        columns: list[str],
    ) -> None:
        now = datetime.now(timezone.utc).isoformat()
        with sqlite_conn(self.db_path) as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO table_metadata (
                    table_name, source_path, file_type, row_count, columns_json, loaded_at
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (table_name, source_path, file_type, row_count, json.dumps(columns), now),
            )
            conn.commit()
