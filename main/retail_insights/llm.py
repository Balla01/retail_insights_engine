from __future__ import annotations

import json
import re
from typing import Any, Optional

from mistralai import Mistral

from .cache import SqlitePromptCache


class MistralClient:
    def __init__(
        self,
        model: str,
        api_key: Optional[str],
        temperature: float,
        cache: SqlitePromptCache,
    ):
        if not api_key:
            raise ValueError("MISTRAL_API_KEY is required.")
        self.model = model
        self.temperature = temperature
        self.cache = cache
        self.api_key = api_key
        self.client = Mistral(api_key=api_key)

    @property
    def is_available(self) -> bool:
        return True

    def _cache_key(self, system_prompt: str, user_prompt: str, expect_json: bool) -> str:
        return f"model={self.model}|json={expect_json}|sys={system_prompt}|usr={user_prompt}"

    def complete(self, system_prompt: str, user_prompt: str, expect_json: bool = False) -> str:
        payload = self._cache_key(system_prompt, user_prompt, expect_json)
        cached = self.cache.get(payload)
        # if cached:
        #     return cached

        response = self.client.chat.complete(
            model=self.model,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            temperature=self.temperature,
        )
        text = (response.choices[0].message.content or "").strip()
        if not text:
            raise ValueError("LLM returned empty response.")
        self.cache.put(payload, text)
        return text

    def complete_json(self, system_prompt: str, user_prompt: str) -> dict[str, Any]:
        raw = self.complete(system_prompt, user_prompt, expect_json=True)
        parsed = self._extract_json(raw)
        if parsed is None:
            raise ValueError(f"Could not parse JSON object from model output: {raw[:400]}")
        return parsed

    @staticmethod
    def _extract_json(text: str) -> Optional[dict[str, Any]]:
        if not text:
            return None

        raw = text.strip()
        candidates: list[str] = []

        fence_blocks = re.findall(r"```(?:json)?\s*(.*?)\s*```", raw, flags=re.DOTALL | re.IGNORECASE)
        candidates.extend(fence_blocks)
        candidates.append(raw)

        for cand in candidates:
            cand = cand.strip()
            if not cand:
                continue

            obj = MistralClient._try_parse_json_dict(cand)
            if obj is not None:
                return obj

            repaired = MistralClient._repair_common_json_issues(cand)
            obj = MistralClient._try_parse_json_dict(repaired)
            if obj is not None:
                return obj

            for blob in MistralClient._extract_brace_balanced_objects(cand):
                obj = MistralClient._try_parse_json_dict(blob)
                if obj is not None:
                    return obj
                repaired_blob = MistralClient._repair_common_json_issues(blob)
                obj = MistralClient._try_parse_json_dict(repaired_blob)
                if obj is not None:
                    return obj

        return None

    @staticmethod
    def _try_parse_json_dict(s: str) -> Optional[dict[str, Any]]:
        s = s.strip()
        try:
            value = json.loads(s)
            return value if isinstance(value, dict) else None
        except Exception:
            return None

    @staticmethod
    def _extract_brace_balanced_objects(s: str) -> list[str]:
        out: list[str] = []
        start: Optional[int] = None
        depth = 0
        in_str = False
        esc = False

        for i, ch in enumerate(s):
            if in_str:
                if esc:
                    esc = False
                elif ch == "\\":
                    esc = True
                elif ch == '"':
                    in_str = False
                continue
            if ch == '"':
                in_str = True
                continue
            if ch == "{":
                if depth == 0:
                    start = i
                depth += 1
            elif ch == "}":
                if depth > 0:
                    depth -= 1
                    if depth == 0 and start is not None:
                        out.append(s[start : i + 1])
                        start = None
        return out

    @staticmethod
    def _repair_common_json_issues(s: str) -> str:
        s = s.replace("“", '"').replace("”", '"').replace("’", "'")

        out_chars: list[str] = []
        in_str = False
        esc = False

        for ch in s:
            if in_str:
                if esc:
                    out_chars.append(ch)
                    esc = False
                    continue
                if ch == "\\":
                    out_chars.append(ch)
                    esc = True
                    continue
                if ch == '"':
                    out_chars.append(ch)
                    in_str = False
                    continue

                if ch == "\n":
                    out_chars.append("\\n")
                elif ch == "\r":
                    out_chars.append("\\r")
                elif ch == "\t":
                    out_chars.append("\\t")
                else:
                    out_chars.append(ch)
            else:
                if ch == '"':
                    out_chars.append(ch)
                    in_str = True
                else:
                    out_chars.append(ch)

        return "".join(out_chars)
