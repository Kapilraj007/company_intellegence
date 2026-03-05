"""
Agent 3 — Validate + Save
==========================
- Runs Pydantic validation against all 163 expected parameters
- Prints a full per-field validation report to terminal
- Saves validated JSON to output/
"""
import json
import os
from datetime import datetime
from typing import Dict, Any

from core.models import validate_golden_record


def save_output(state: Dict[str, Any]) -> Dict[str, Any]:
    golden_record = state.get("golden_record", [])
    company_name  = state.get("company_name", "unknown")

    print(f"\n[Save] Rows received: {len(golden_record)}")

    if not golden_record:
        print("[Save] ⚠️  Empty golden record — nothing to validate or save.")
        return {}

    # ── Run full 163-field Pydantic validation ────────────────────────────
    valid_rows, report = validate_golden_record(golden_record, company_name)

    # ── Print the full validation report to terminal ──────────────────────
    report.print_report()

    if not valid_rows:
        print("[Save] ❌ No valid rows after validation — skipping file write.")
        return {}

    # ── Write validated output to file ────────────────────────────────────
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    output_dir   = os.path.join(project_root, "output")
    os.makedirs(output_dir, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    safe_name = company_name.lower().replace(" ", "_")

    # Save validated data
    data_file = os.path.join(output_dir, f"{safe_name}_golden_record_{timestamp}.json")
    with open(data_file, "w", encoding="utf-8") as f:
        json.dump(valid_rows, f, indent=2, ensure_ascii=False)

    # Save validation report as JSON too
    report_file = os.path.join(output_dir, f"{safe_name}_validation_report_{timestamp}.json")
    with open(report_file, "w", encoding="utf-8") as f:
        json.dump(report.model_dump(), f, indent=2, ensure_ascii=False)

    print(f"[Save] ✅ Golden record  → {data_file}")
    print(f"[Save] 📋 Validation report → {report_file}")
    print(f"[Save] 📊 {report.total_passed}/163 fields passed ({report.completeness_pct}% complete)")

    return {}