import os
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv


load_dotenv()


@dataclass(frozen=True)
class AssistantConfig:
    db_path: str = os.getenv("SQLITE_DB_PATH", "./retail_insights.db")
    mistral_api_key: Optional[str] = os.getenv("MISTRAL_API_KEY")
    mistral_model: str = os.getenv("MISTRAL_MODEL", "mistral-medium-latest")
    llm_temperature: float = float(os.getenv("LLM_TEMPERATURE", "0.2"))
    max_sql_rows: int = int(os.getenv("MAX_SQL_ROWS", "200"))
    memory_turns: int = int(os.getenv("MEMORY_TURNS", "8"))
    chroma_persist_path: str = os.getenv("CHROMA_PERSIST_PATH", "./chroma_db")
    embedding_model_name: str = os.getenv("EMBEDDING_MODEL_NAME", "Alibaba-NLP/gte-large-en-v1.5")

    @property
    def db_parent(self) -> Path:
        return Path(self.db_path).resolve().parent


DEFAULT_CONFIG = AssistantConfig()
