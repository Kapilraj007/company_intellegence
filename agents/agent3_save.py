"""
Agent 3 — Validate + Save

INPUT:  golden_record — single flat JSON object (163 keys)
OUTPUT: golden_record_path (flat object), validation_report_path

Runs full 163-field Pydantic validation via validate_flat_golden_record().
Persists all run artifacts locally.
"""

from __future__ import annotations

import json
import os
import re
from datetime import datetime
from typing import Any, Dict

from core.chunking import generate_semantic_chunks
from core.local_store import get_local_store_client
from core.models import validate_flat_golden_record
from core.user_scope import require_user_id
from logger import get_logger

logger = get_logger("agent3_save")


def _slug(value: str) -> str:
    cleaned = re.sub(r"[^a-zA-Z0-9]+", "_", (value or "").strip().lower())
    cleaned = re.sub(r"_+", "_", cleaned).strip("_")
    return cleaned or "unknown"


def _rows_to_flat(rows: list[dict[str, Any]]) -> Dict[str, str]:
    """Legacy adapter: row-list payload -> flat object."""
    from core.prompts import _FLAT_KEYS

    flat: Dict[str, str] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        try:
            field_id = int(row.get("ID", 0))
        except (TypeError, ValueError):
            continue
        key = _FLAT_KEYS.get(field_id)
        if not key:
            continue
        value = row.get("Research Output / Data")
        if value is None:
            value = row.get("Research Output")
        if value is None:
            value = row.get("Data")
        flat[key] = str(value).strip() if value is not None else "Not Found"
    return flat


def _normalize_golden_record(payload: Any) -> Dict[str, str]:
    if isinstance(payload, dict):
        return {str(k): str(v) if v is not None else "Not Found" for k, v in payload.items()}
    if isinstance(payload, list):
        logger.warning("[Agent3] Received legacy row-list payload, converting to flat object.")
        return _rows_to_flat(payload)
    return {}


def save_output(state: Dict[str, Any]) -> Dict[str, Any]:
    golden_record = _normalize_golden_record(state.get("golden_record", {}))
    company_name = str(state.get("company_name", "unknown") or "unknown").strip()
    user_id = require_user_id(state.get("user_id"), context="Agent3 save_output")

    filled = sum(
        1
        for v in golden_record.values()
        if str(v).strip().lower() not in {"not found", "n/a", "unknown", "none", ""}
    )
    print(f"\n[Agent3] Keys received: {len(golden_record)} ({filled} filled)")

    if not golden_record:
        print("[Agent3] Empty golden record — nothing to validate or save.")
        return {
            "golden_record": {},
            "golden_record_path": None,
            "flat_record_path": None,
            "validation_report_path": None,
        }

    valid_flat, report = validate_flat_golden_record(golden_record, company_name)
    report.print_report()

    if not valid_flat:
        print("[Agent3] No valid data after validation — skipping file write.")
        return {
            "golden_record": {},
            "golden_record_path": None,
            "flat_record_path": None,
            "validation_report_path": None,
        }

    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    output_dir = os.path.join(project_root, "output")
    os.makedirs(output_dir, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    safe_name = _slug(company_name)

    golden_path = os.path.join(output_dir, f"{safe_name}_golden_record_{timestamp}.json")
    with open(golden_path, "w", encoding="utf-8") as f:
        json.dump(valid_flat, f, indent=2, ensure_ascii=False)

    report_path = os.path.join(output_dir, f"{safe_name}_validation_report_{timestamp}.json")
    with open(report_path, "w", encoding="utf-8") as f:
        json.dump(report.model_dump(), f, indent=2, ensure_ascii=False)

    print(f"[Agent3] Golden record (flat) -> {golden_path}")
    print(f"[Agent3] Validation report     -> {report_path}")
    print(
        f"[Agent3] {report.total_passed}/163 fields validated "
        f"({report.completeness_pct}% complete)\n"
    )

    run_id = str(state.get("run_id") or f"local-{safe_name}-{timestamp}")
    company_id = str(state.get("company_id") or safe_name)
    test_results = state.get("test_results", {}) or {}

    chunk_payload = generate_semantic_chunks(company_name, valid_flat)
    semantic_chunks = list(chunk_payload.get("chunks") or [])
    chunk_coverage = dict(chunk_payload.get("coverage") or {})
    chunk_path = os.path.join(output_dir, f"{safe_name}_semantic_chunks_{timestamp}.json")
    with open(chunk_path, "w", encoding="utf-8") as f:
        json.dump(
            {
                "company_id": company_id,
                "company_name": company_name,
                "user_id": user_id,
                "run_id": run_id,
                "generated_at": datetime.utcnow().isoformat() + "Z",
                "chunk_count": len(semantic_chunks),
                "coverage": chunk_coverage,
                "chunks": semantic_chunks,
            },
            f,
            indent=2,
            ensure_ascii=False,
        )

    print(f"[Agent3] Semantic chunks       -> {chunk_path}")

    try:
        store = get_local_store_client()
        store.upsert_company_consolidated_data(
            run_id=run_id,
            company_name=company_name,
            company_id=company_id,
            user_id=user_id,
            consolidated_json=valid_flat,
            chunk_count=len(semantic_chunks),
            chunk_coverage_pct=float(chunk_coverage.get("coverage_pct") or 0.0),
        )
        store.insert_company_chunks(
            run_id=run_id,
            company_name=company_name,
            company_id=company_id,
            user_id=user_id,
            chunks=semantic_chunks,
        )
        store.complete_pipeline_run(
            run_id=run_id,
            user_id=user_id,
            golden_record_count=report.total_passed,
            all_tests_passed=bool(test_results.get("all_passed", False)),
            failed_param_ids=list(test_results.get("failed_parameter_ids", []) or []),
            agent2_retry_count=int(state.get("retry_consolidation", 0) or 0),
            pytest_retry_count=int(state.get("pytest_retry_count", 0) or 0),
            golden_record_path=golden_path,
            validation_path=report_path,
            pytest_report_path=state.get("pytest_report_path"),
        )
    except Exception as exc:
        logger.warning(f"Local store persistence failed (non-fatal): {exc}")

    # Pinecone: store consolidated golden record as category vectors.
    # Only the validated Agent 2 output goes here. Agent 1 data never touches Pinecone.
    try:
        from core.pinecone_store import get_pinecone_client
        vector_count = get_pinecone_client().upsert_golden_record(
            run_id=run_id,
            company_id=company_id,
            company_name=company_name,
            golden_record=valid_flat,
            user_id=user_id,
        )
        print(f"[Agent3] Pinecone: {vector_count} category vectors upserted.")
    except Exception as exc:
        logger.warning(f"Pinecone upsert failed (non-fatal): {exc}")

    try:
        from core.supabase_store import get_supabase_client

        get_supabase_client().complete_pipeline_run(run_id=run_id, user_id=user_id)
    except Exception as exc:
        logger.warning(f"Supabase run completion failed (non-fatal): {exc}")

    return {
        "golden_record": valid_flat,
        "golden_record_sources": state.get("golden_record_sources", {}),
        "golden_record_path": golden_path,
        "flat_record_path": golden_path,
        "validation_report_path": report_path,
        "chunk_record_path": chunk_path,
        "semantic_chunks": semantic_chunks,
        "chunk_coverage": chunk_coverage,
    }
