"""
Semantic chunk generation for 163-field consolidated company records.

The chunker builds 10 stable chunks to support downstream retrieval/indexing.
"""

from __future__ import annotations

import re
from typing import Any, Dict, List

from core.prompts import _FLAT_KEYS

EXPECTED_FIELD_COUNT = 163
DEFAULT_CHUNK_COUNT = 10
MIN_WORDS = 60
MAX_WORDS = 120

_FLAT_KEY_TO_ID = {key: field_id for field_id, key in _FLAT_KEYS.items()}
_WORD_RE = re.compile(r"\b[\w'-]+\b")


def _word_count(text: str) -> int:
    return len(_WORD_RE.findall(text or ""))


def _normalize_value(value: Any) -> str:
    if value is None:
        return "Not Found"
    text = str(value).strip()
    return text or "Not Found"


def _shorten_words(text: str, max_words: int = 3) -> str:
    words = [token for token in re.split(r"\s+", text.strip()) if token]
    if not words:
        return "Not Found"
    return " ".join(words[:max_words])


def _split_ids_evenly(total: int, chunk_count: int) -> List[List[int]]:
    base = total // chunk_count
    remainder = total % chunk_count

    start = 1
    chunks: List[List[int]] = []
    for idx in range(chunk_count):
        size = base + (1 if idx < remainder else 0)
        end = start + size - 1
        chunks.append(list(range(start, end + 1)))
        start = end + 1
    return chunks


def _build_chunk_text(
    *,
    company_name: str,
    chunk_title: str,
    field_ids: List[int],
    flat_record: Dict[str, Any],
) -> str:
    start_id = field_ids[0]
    end_id = field_ids[-1]
    lines = [
        (
            f"{chunk_title}: {company_name} profile window {start_id}-{end_id}. "
            f"Each token pairs field id, key, and short value."
        )
    ]

    for field_id in field_ids:
        key = _FLAT_KEYS[field_id]
        value = _shorten_words(_normalize_value(flat_record.get(key, "Not Found")))
        lines.append(f"f{field_id}:{key}={value};")

    text = "\n".join(lines)
    words = _word_count(text)

    while words < MIN_WORDS:
        lines.append("ctx: unavailable details.")
        text = "\n".join(lines)
        words = _word_count(text)

    while words > MAX_WORDS and len(lines) > 2:
        lines.pop()
        text = "\n".join(lines)
        words = _word_count(text)

    return text


def generate_semantic_chunks(
    company_name: str,
    flat_record: Dict[str, Any],
    *,
    chunk_count: int = DEFAULT_CHUNK_COUNT,
) -> Dict[str, Any]:
    """
    Build deterministic semantic chunks from a flat 163-field golden record.
    """
    safe_company = str(company_name or "unknown").strip() or "unknown"
    record = flat_record if isinstance(flat_record, dict) else {}

    id_groups = _split_ids_evenly(EXPECTED_FIELD_COUNT, max(1, int(chunk_count)))
    chunks: List[Dict[str, Any]] = []

    for idx, field_ids in enumerate(id_groups, start=1):
        chunk_title = f"Company Profile Chunk {idx:02d}"
        chunk_text = _build_chunk_text(
            company_name=safe_company,
            chunk_title=chunk_title,
            field_ids=field_ids,
            flat_record=record,
        )

        source_fields = [_FLAT_KEYS[field_id] for field_id in field_ids]
        chunks.append(
            {
                "chunk_id": f"{safe_company.lower().replace(' ', '_')}_chunk_{idx:02d}",
                "chunk_index": idx,
                "chunk_title": chunk_title,
                "chunk_type": "semantic_profile_bundle",
                "chunk_text": chunk_text,
                "word_count": _word_count(chunk_text),
                "source_field_ids": field_ids,
                "source_fields": source_fields,
            }
        )

    covered_ids = sorted(
        {
            _FLAT_KEY_TO_ID[key]
            for key in record.keys()
            if isinstance(key, str) and key in _FLAT_KEY_TO_ID
        }
    )
    expected_ids = set(range(1, EXPECTED_FIELD_COUNT + 1))
    missing_ids = sorted(expected_ids.difference(covered_ids))

    coverage = {
        "covered_field_count": len(covered_ids),
        "expected_field_count": EXPECTED_FIELD_COUNT,
        "coverage_pct": round((len(covered_ids) / EXPECTED_FIELD_COUNT) * 100, 1),
        "missing_field_ids": missing_ids,
    }

    return {"chunks": chunks, "coverage": coverage}
