"""
Agent 3 — Validate + Save
Runs full 163-field Pydantic validation and prints a detailed report.
Saves golden_record JSON and validation_report JSON to output/.
Returns file paths into GraphState so Agent 4 (test runner) can consume them.
"""
import json
import os
from datetime import datetime
from typing import Dict, Any

from core.models import validate_golden_record


def save_output(state: Dict[str, Any]) -> Dict[str, Any]:
    golden_record = state.get("golden_record", [])
    company_name  = state.get("company_name", "unknown")

    print(f"\n[Agent3] Rows received: {len(golden_record)}")

    if not golden_record:
        print("[Agent3] ⚠️  Empty golden record — nothing to validate or save.")
        return {"golden_record_path": None, "validation_report_path": None}

    # ── Run Pydantic validation on all 163 fields ─────────────────────────
    valid_rows, report = validate_golden_record(golden_record, company_name)

    # ── Print full validation report to terminal ──────────────────────────
    report.print_report()

    if not valid_rows:
        print("[Agent3] ❌ No valid rows after validation — skipping file write.")
        return {"golden_record_path": None, "validation_report_path": None}

    # ── Write output files ────────────────────────────────────────────────
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    output_dir   = os.path.join(project_root, "output")
    os.makedirs(output_dir, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    safe_name = company_name.lower().replace(" ", "_")

    # Golden record
    data_path = os.path.join(output_dir, f"{safe_name}_golden_record_{timestamp}.json")
    with open(data_path, "w", encoding="utf-8") as f:
        json.dump(valid_rows, f, indent=2, ensure_ascii=False)

    # Validation report
    report_path = os.path.join(output_dir, f"{safe_name}_validation_report_{timestamp}.json")
    with open(report_path, "w", encoding="utf-8") as f:
        json.dump(report.model_dump(), f, indent=2, ensure_ascii=False)

    print(f"[Agent3] ✅ Golden record     → {data_path}")
    print(f"[Agent3] 📋 Validation report → {report_path}")
    print(f"[Agent3] 📊 {report.total_passed}/163 fields validated ({report.completeness_pct}% complete)\n")

    # ── Return paths into state for Agent 4 ──────────────────────────────
    return {
        "golden_record_path":     data_path,
        "validation_report_path": report_path,
    }