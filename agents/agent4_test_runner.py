"""
Agent 4 — Pytest Runner Node
=============================
LangGraph node that runs pytest after Agent 3 saves output.
If pytest fails with parameter-linked issues, graph can route back for
targeted Agent 1 retries before re-consolidating and re-testing.

Fixes applied:
  - Explicit absolute path passed to pytest (no reliance on cwd)
  - --import-mode=importlib avoids sys.path conflicts with LangGraph's loaded modules
  - -v instead of -q so collection is visible in terminal
  - sys.path pre-seeded so conftest.py can import core/
  - Diagnostic prints show exactly what pytest is doing
"""
import json
import os
import re
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Set

import pytest
from core.test_reports import PytestReport

# ── Ensure project root is on sys.path before pytest collects anything ─────────
_PROJECT_ROOT = Path(__file__).parent.parent.resolve()
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

_ID_PATTERNS = (
    re.compile(r"\bID\s*[:=]\s*(\d{1,3})\b", flags=re.IGNORECASE),
    re.compile(r"\bID\s+(\d{1,3})\b", flags=re.IGNORECASE),
    re.compile(r"\bid_(\d{1,3})\b", flags=re.IGNORECASE),
)


# ── Custom pytest plugin — collects pass/fail without parsing stdout ───────────

class ResultCollector:
    """Minimal pytest plugin that accumulates test outcomes in memory."""

    def __init__(self):
        self.passed:  List[str]  = []
        self.failed:  List[Dict] = []
        self.skipped: List[str]  = []
        self.errors:  List[Dict] = []
        self._seen_skips: Set[str] = set()
        self._seen_errors: Set[str] = set()
        self.start_time: float = 0.0
        self.duration:   float = 0.0

    def pytest_sessionstart(self, session):
        self.start_time = time.perf_counter()

    def pytest_sessionfinish(self, session, exitstatus):
        self.duration = round(time.perf_counter() - self.start_time, 2)

    def pytest_runtest_logreport(self, report):
        if report.when == "setup" and report.failed:
            if report.nodeid not in self._seen_errors:
                self.errors.append({
                    "name":    report.nodeid,
                    "phase":   "setup",
                    "message": str(report.longrepr),
                })
                self._seen_errors.add(report.nodeid)
            return
        if report.when == "setup" and report.skipped:
            if report.nodeid not in self._seen_skips:
                self.skipped.append(report.nodeid)
                self._seen_skips.add(report.nodeid)
            return
        if report.when != "call":
            return
        if report.passed:
            self.passed.append(report.nodeid)
        elif report.failed:
            self.failed.append({
                "name":    report.nodeid,
                "message": str(report.longrepr),
            })
        elif report.skipped:
            if report.nodeid not in self._seen_skips:
                self.skipped.append(report.nodeid)
                self._seen_skips.add(report.nodeid)

    @property
    def summary(self) -> Dict[str, Any]:
        total = len(self.passed) + len(self.failed) + len(self.skipped)
        return {
            "total":        total,
            "passed":       len(self.passed),
            "failed":       len(self.failed),
            "skipped":      len(self.skipped),
            "errors":       len(self.errors),
            "duration_sec": self.duration,
            "all_passed":   len(self.failed) == 0 and len(self.errors) == 0,
            "failed_tests": self.failed,
            "error_tests":  self.errors,
        }


def _extract_ids_from_text(text: str) -> Set[int]:
    ids: Set[int] = set()
    if not text:
        return ids

    for pattern in _ID_PATTERNS:
        for match in pattern.findall(text):
            try:
                value = int(match)
            except (TypeError, ValueError):
                continue
            if 1 <= value <= 163:
                ids.add(value)
    return ids


def extract_failed_parameter_ids(test_results: Dict[str, Any]) -> List[int]:
    """
    Parse pytest failures/errors and map them to parameter IDs (1..163).
    Only IDs with explicit context (ID=..., ID 77, id_77) are extracted.
    """
    ids: Set[int] = set()

    for issue in test_results.get("failed_tests", []):
        ids.update(_extract_ids_from_text(issue.get("name", "")))
        ids.update(_extract_ids_from_text(issue.get("message", "")))

    for issue in test_results.get("error_tests", []):
        ids.update(_extract_ids_from_text(issue.get("name", "")))
        ids.update(_extract_ids_from_text(issue.get("message", "")))

    return sorted(ids)


def route_after_tests(state: Dict[str, Any]) -> str:
    """
    LangGraph router after run_tests.
    - end: pytest passed OR no parameter-linked failures
    - retry_via_llm1: run targeted Agent 1 remediation loop
    """
    test_results = state.get("test_results", {})
    if test_results.get("all_passed"):
        print("[Agent4 Router] All tests passed → END")
        return "end"

    failed_ids = test_results.get("failed_parameter_ids")
    if failed_ids is None:
        failed_ids = extract_failed_parameter_ids(test_results)

    if not failed_ids:
        print("[Agent4 Router] No parameter-linked failures extracted → END")
        return "end"

    print(
        f"[Agent4 Router] Parameter failures {failed_ids} → send to Agent1 targeted regeneration"
    )
    return "retry_via_llm1"


# ── The LangGraph node ─────────────────────────────────────────────────────────

def run_tests(state: Dict[str, Any]) -> Dict[str, Any]:
    """
    LangGraph node: runs pytest suite against the output of the current pipeline run.
    Always returns — never raises — so the pipeline completes even if tests fail.
    """
    golden_record_path = state.get("golden_record_path")
    company_name       = state.get("company_name", "unknown")

    _print_header(company_name)

    # ── Resolve paths ──────────────────────────────────────────────────────
    project_root = _PROJECT_ROOT
    tests_dir    = project_root / "tests"

    print(f"[Agent4] Project root : {project_root}")
    print(f"[Agent4] Tests dir    : {tests_dir}")
    print(f"[Agent4] Tests exists : {tests_dir.exists()}")
    print(f"[Agent4] Golden path  : {golden_record_path}")

    # ── Guard: no output to test ───────────────────────────────────────────
    if not golden_record_path or not Path(golden_record_path).exists():
        msg = (f"golden_record_path not set or missing: '{golden_record_path}'. "
               "Agent 3 may have produced no valid rows.")
        print(f"\n[Agent4] ⚠️  {msg}\n")
        results = {
            "total": 0, "passed": 0, "failed": 0, "skipped": 0,
            "errors": 0, "duration_sec": 0.0, "all_passed": False,
            "failed_tests": [], "error_tests": [],
            "failed_parameter_ids": [],
            "skip_reason": msg,
        }
        report_path = _save_pytest_report(
            project_root=project_root,
            company_name=company_name,
            golden_record_path=golden_record_path,
            results=results,
            exit_code=2,
        )
        return {"test_results": results, "pytest_report_path": report_path}

    if not tests_dir.exists():
        msg = f"tests/ directory not found at: {tests_dir}"
        print(f"[Agent4] ❌ {msg}")
        results = {
            "total": 0, "passed": 0, "failed": 1, "all_passed": False,
            "skip_reason": msg,
            "skipped": 0, "errors": 0, "duration_sec": 0.0,
            "failed_tests": [], "error_tests": [],
            "failed_parameter_ids": [],
        }
        report_path = _save_pytest_report(
            project_root=project_root,
            company_name=company_name,
            golden_record_path=golden_record_path,
            results=results,
            exit_code=2,
        )
        return {"test_results": results, "pytest_report_path": report_path}

    # ── List what pytest will discover (diagnostic) ────────────────────────
    test_files = list(tests_dir.rglob("test_*.py"))
    print(f"\n[Agent4] Test files found ({len(test_files)}):")
    for f in sorted(test_files):
        print(f"         {f.relative_to(project_root)}")

    # ── Inject live context for integration tests ──────────────────────────
    os.environ["LIVE_GOLDEN_RECORD_PATH"] = str(golden_record_path)
    os.environ["LIVE_COMPANY_NAME"]       = company_name

    # ── Build pytest args ──────────────────────────────────────────────────
    pytest_args = [
        str(tests_dir),                        # explicit absolute path to tests/
        "--tb=short",                           # concise tracebacks
        "-v",                                   # verbose: shows each test name
        "--no-header",
        f"--rootdir={project_root}",            # tell pytest where project root is
        f"--import-mode=importlib",             # avoid sys.path conflicts with langgraph
        f"--override-ini=pythonpath={project_root}",  # ensure core/ importable
    ]

    print(f"\n[Agent4] pytest args: {' '.join(pytest_args)}\n")

    # ── Run pytest ─────────────────────────────────────────────────────────
    collector = ResultCollector()
    exit_code = -1

    try:
        exit_code = pytest.main(pytest_args, plugins=[collector])
    except SystemExit as e:
        exit_code = e.code
    except Exception as e:
        print(f"[Agent4] ❌ pytest runtime error: {e}")
        results = {
            "total": 0, "passed": 0, "failed": 1, "all_passed": False,
            "skip_reason": f"pytest runtime error: {e}",
            "skipped": 0, "errors": 1, "duration_sec": 0.0,
            "failed_tests": [], "error_tests": [],
            "failed_parameter_ids": [],
        }
        report_path = _save_pytest_report(
            project_root=project_root,
            company_name=company_name,
            golden_record_path=golden_record_path,
            results=results,
            exit_code=1,
        )
        return {"test_results": results, "pytest_report_path": report_path}

    print(f"\n[Agent4] pytest exit code: {exit_code}")

    results = collector.summary
    results["failed_parameter_ids"] = extract_failed_parameter_ids(results)
    if results["failed_parameter_ids"]:
        print(f"[Agent4] Parameter-linked failures: {results['failed_parameter_ids']}")
    _print_summary(results, company_name)
    report_path = _save_pytest_report(
        project_root=project_root,
        company_name=company_name,
        golden_record_path=golden_record_path,
        results=results,
        exit_code=exit_code,
    )

    # ── Clean up env vars ──────────────────────────────────────────────────
    os.environ.pop("LIVE_GOLDEN_RECORD_PATH", None)
    os.environ.pop("LIVE_COMPANY_NAME",       None)

    return {"test_results": results, "pytest_report_path": report_path}


# ── Print helpers ──────────────────────────────────────────────────────────────

def _print_header(company_name: str):
    sep = "=" * 70
    print(f"\n{sep}")
    print(f"  AGENT 4 — PYTEST TEST RUNNER")
    print(f"  Company : {company_name}")
    print(f"  Sections: 12.5 · 13.2 · 13.3 · 13.4 · 14.1 · 14.2")
    print(f"{sep}\n")


def _print_summary(results: Dict[str, Any], company_name: str):
    sep  = "=" * 70
    icon = "✅" if results["all_passed"] else "❌"
    print(f"\n{sep}")
    print(f"  {icon} PYTEST RESULTS — {company_name.upper()}")
    print(f"{sep}")
    print(f"  Total    : {results['total']}")
    print(f"  ✅ Passed : {results['passed']}")
    print(f"  ❌ Failed : {results['failed']}")
    print(f"  ⏭  Skipped: {results['skipped']}")
    print(f"  ⚠️  Errors : {results['errors']}")
    print(f"  Duration : {results['duration_sec']}s")
    print(f"{sep}")

    if results["failed_tests"]:
        print(f"\n  ❌ FAILED TESTS ({len(results['failed_tests'])}):")
        for t in results["failed_tests"]:
            print(f"\n  ▶ {t['name']}")
            for line in t["message"].split("\n")[:8]:
                print(f"    {line}")

    if results["error_tests"]:
        print(f"\n  ⚠️  SETUP ERRORS ({len(results['error_tests'])}):")
        for t in results["error_tests"]:
            print(f"  ▶ {t['name']}: {t['message'][:200]}")

    if results.get("failed_parameter_ids"):
        print(f"\n  🔁 Failed parameter IDs: {results['failed_parameter_ids']}")

    status = "ALL TESTS PASSED ✅" if results["all_passed"] else "SOME TESTS FAILED ❌"
    print(f"\n  → {status}")
    print(f"{sep}\n")


def _save_pytest_report(
    project_root: Path,
    company_name: str,
    golden_record_path: str | None,
    results: Dict[str, Any],
    exit_code: int,
) -> str:
    output_dir = project_root / "output"
    output_dir.mkdir(parents=True, exist_ok=True)

    timestamp = _extract_timestamp(golden_record_path) or datetime.now().strftime("%Y%m%d_%H%M%S")
    safe_name = company_name.lower().replace(" ", "_")
    report_path = output_dir / f"{safe_name}_pytest_report_{timestamp}.json"

    report = PytestReport.model_validate({
        "company_name": company_name,
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "golden_record_path": golden_record_path,
        "total": results.get("total", 0),
        "passed": results.get("passed", 0),
        "failed": results.get("failed", 0),
        "skipped": results.get("skipped", 0),
        "errors": results.get("errors", 0),
        "duration_sec": results.get("duration_sec", 0.0),
        "all_passed": results.get("all_passed", False),
        "failed_tests": results.get("failed_tests", []),
        "error_tests": results.get("error_tests", []),
        "failed_parameter_ids": results.get("failed_parameter_ids", []),
        "exit_code": int(exit_code),
        "skip_reason": results.get("skip_reason"),
    })

    with open(report_path, "w", encoding="utf-8") as f:
        json.dump(report.model_dump(), f, indent=2, ensure_ascii=False)

    print(f"[Agent4] 🧪 Pytest report     → {report_path}")
    return str(report_path)


def _extract_timestamp(golden_record_path: str | None) -> str | None:
    if not golden_record_path:
        return None
    name = Path(golden_record_path).name
    match = re.search(r"_(\d{8}_\d{6})\.json$", name)
    if not match:
        return None
    return match.group(1)
