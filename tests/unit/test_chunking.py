"""Unit tests for semantic chunk generation."""

from core.chunking import generate_semantic_chunks
from core.prompts import _FLAT_KEYS


def _build_flat_record() -> dict:
    record = {}
    for field_id, key in _FLAT_KEYS.items():
        if field_id % 5 == 0:
            record[key] = "Not Found"
        else:
            record[key] = f"Value for {key}"
    record["company_name"] = "Tesla"
    return record


def test_generate_semantic_chunks_count_and_coverage():
    payload = generate_semantic_chunks("Tesla", _build_flat_record())

    chunks = payload["chunks"]
    coverage = payload["coverage"]

    assert 8 <= len(chunks) <= 12
    assert len(chunks) == 10
    assert coverage["covered_field_count"] == 163
    assert coverage["missing_field_ids"] == []


def test_generate_semantic_chunks_word_limits_and_metadata():
    payload = generate_semantic_chunks("Tesla", _build_flat_record())
    chunks = payload["chunks"]

    for chunk in chunks:
        assert 60 <= int(chunk["word_count"]) <= 120
        assert chunk["chunk_type"]
        assert chunk["chunk_text"].startswith(f'{chunk["chunk_title"]}:')
        assert chunk["source_field_ids"]
        assert chunk["source_fields"]
