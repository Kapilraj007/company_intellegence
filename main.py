"""
Main entry point for:
1) full pipeline run (default), and
2) targeted regeneration-only runs.
"""
import argparse
import json
from pathlib import Path
from typing import Any, Dict, Iterable, List

import uuid
from dotenv import load_dotenv
from logger import get_logger
from core.local_store import get_local_store_client
from core.user_scope import require_user_id
from search.search_service import get_search_service

from agents.agent1_research import run_targeted_research
from agents.agent2_consolidation import (
    check_consolidation,
    inc_retry_consolidation,
    run_consolidation,
)
from agents.agent3_save import save_output
from agents.agent4_test_runner import extract_failed_parameter_ids, route_after_tests, run_tests
from graph import graph

load_dotenv()
logger = get_logger("main")


def _make_company_id(company_name: str) -> str:
    safe = "".join(ch if ch.isalnum() else "_" for ch in company_name.lower())
    return "_".join(part for part in safe.split("_") if part) or "unknown"


def _normalize_ids(values: Iterable[Any]) -> List[int]:
    ids = set()
    for raw in values:
        try:
            value = int(raw)
        except (TypeError, ValueError):
            continue
        if 1 <= value <= 163:
            ids.add(value)
    return sorted(ids)


def _parse_failed_ids(csv_ids: str) -> List[int]:
    if not csv_ids.strip():
        return []
    return _normalize_ids(part.strip() for part in csv_ids.split(","))


def _load_json(path: str) -> Any:
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def _load_failed_ids_from_report(report_path: str) -> List[int]:
    data = _load_json(report_path)
    if not isinstance(data, dict):
        raise ValueError(f"Invalid pytest report JSON at: {report_path}")
    return _normalize_ids(data.get("failed_parameter_ids", []))


def _load_base_record(base_record_path: str) -> List[Dict[str, Any]]:
    data = _load_json(base_record_path)
    if isinstance(data, dict):
        # New canonical format: one flat object (163 keys).
        return [data]
    if isinstance(data, list):
        rows: List[Dict[str, Any]] = []
        for row in data:
            if isinstance(row, dict):
                rows.append(row)
        if rows:
            return rows
    raise ValueError(f"Base record must be a JSON object or list: {base_record_path}")


def _register_supabase_run(
    run_id: str,
    company_name: str,
    company_id: str,
    *,
    user_id: str,
    user_name: str = "",
) -> None:
    user_id = require_user_id(user_id, context="Supabase run registration")
    try:
        from core.supabase_store import get_supabase_client

        get_supabase_client().create_pipeline_run(
            run_id=run_id,
            company_name=company_name,
            company_id=company_id,
            user_id=user_id,
            user_name=user_name,
        )
    except Exception as exc:
        logger.warning(f"Could not register Supabase pipeline run: {exc}")


def run_full_pipeline(company_name: str, *, user_id: str, user_name: str = "") -> Dict[str, Any]:
    user_id = require_user_id(user_id, context="full pipeline")
    sep = "=" * 70
    print(f"\n{sep}")
    print("  Company Intelligence Pipeline + Test Suite")
    print(f"  Target   : {company_name}")
    print("  Mode     : full")
    print(f"{sep}\n")
    run_id     = str(uuid.uuid4())
    company_id = _make_company_id(company_name)

    # Register run in local store.
    try:
        store = get_local_store_client()
        store.create_pipeline_run(
            run_id=run_id,
            company_name=company_name,
            company_id=company_id,
            user_id=user_id,
            user_name=user_name,
        )
    except Exception as e:
        logger.warning(f"Could not register local pipeline run: {e}")

    # Register run in Supabase for Agent1 FK integrity.
    try:
        _register_supabase_run(run_id, company_name, company_id, user_id=user_id, user_name=user_name)
    except TypeError:
        # Backward-compatible for tests/mocks that still patch the legacy signature.
        _register_supabase_run(run_id, company_name, company_id, user_id=user_id)

    # Add run_id, company_id, and user_id to the LangGraph initial state.
    # One graph invocation already includes the internal retry/remediation loops.
    result = graph.invoke(
        {
            "company_name": company_name,
            "user_id": user_id,
            "run_id": run_id,
            "company_id": company_id,
        },
        config={"recursion_limit": 500},
    )
    _print_summary(result, company_name, "PIPELINE COMPLETE")
    return result


def run_regeneration_only(
    company_name: str,
    base_record_path: str,
    failed_ids: List[int],
    max_rounds: int,
    run_pytests: bool,
    *,
    user_id: str,
    user_name: str = "",
) -> Dict[str, Any]:
    user_id = require_user_id(user_id, context="targeted regeneration")
    sep = "=" * 70
    base_rows = _load_base_record(base_record_path)
    run_id = str(uuid.uuid4())
    company_id = _make_company_id(company_name)

    state: Dict[str, Any] = {
        "company_name": company_name,
        "user_id": user_id,
        "run_id": run_id,
        "company_id": company_id,
        # Use previously saved golden record as baseline candidates.
        "combined_raw": base_rows,
        "retry_consolidation": 0,
        "pytest_retry_count": 0,
        "test_results": {
            "all_passed": False,
            "failed_parameter_ids": _normalize_ids(failed_ids),
        },
    }

    try:
        store = get_local_store_client()
        store.create_pipeline_run(
            run_id=run_id,
            company_name=company_name,
            company_id=company_id,
            user_id=user_id,
            user_name=user_name,
        )
    except Exception as exc:
        logger.warning(f"Could not register regen run locally: {exc}")

    try:
        _register_supabase_run(run_id, company_name, company_id, user_id=user_id, user_name=user_name)
    except TypeError:
        _register_supabase_run(run_id, company_name, company_id, user_id=user_id)

    print(f"\n{sep}")
    print("  Company Intelligence Targeted Regeneration")
    print(f"  Target       : {company_name}")
    print("  Mode         : regen")
    print(f"  Base record  : {base_record_path}")
    print(f"  Failed IDs   : {state['test_results']['failed_parameter_ids']}")
    print(f"  Max rounds   : {max_rounds}")
    print(f"  Run tests    : {run_pytests}")
    print(f"{sep}\n")

    for round_idx in range(1, max_rounds + 1):
        current_ids = _normalize_ids(
            state.get("test_results", {}).get("failed_parameter_ids", [])
        )
        if not current_ids:
            print(f"[Regen] No failed IDs left before round {round_idx}; stopping.")
            break

        print(f"\n[Regen] Round {round_idx}/{max_rounds} for IDs: {current_ids}")

        # Agent1 targeted fetch for failed IDs.
        state.update(run_targeted_research(state))

        # Agent2 consolidation with its retry router.
        while True:
            state.update(run_consolidation(state))
            decision = check_consolidation(state)
            if decision == "save":
                break
            state.update(inc_retry_consolidation(state))

        # Agent3 save.
        state.update(save_output(state))

        if not run_pytests:
            continue

        # Agent4 re-test + route decision.
        state.update(run_tests(state))
        if "failed_parameter_ids" not in (state.get("test_results") or {}):
            state["test_results"]["failed_parameter_ids"] = extract_failed_parameter_ids(
                state["test_results"]
            )

        next_step = route_after_tests(state)
        if next_step == "end":
            print("[Regen] Agent4 router returned end; stopping.")
            break

    _print_summary(state, company_name, "REGEN COMPLETE")
    return state


def show_graph_flow(graph_dir: str, export_png: bool) -> Dict[str, str | None]:
    sep = "=" * 70
    out_dir = Path(graph_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    graph_obj = graph.get_graph()
    ascii_diagram = None
    ascii_error = None
    try:
        ascii_diagram = graph_obj.draw_ascii()
    except Exception as exc:
        ascii_error = str(exc)
    mermaid_diagram = graph_obj.draw_mermaid()

    ascii_path = out_dir / "langgraph_flow_ascii.txt"
    mermaid_path = out_dir / "langgraph_flow.mmd"
    if ascii_diagram is not None:
        ascii_path.write_text(ascii_diagram, encoding="utf-8")
    else:
        ascii_path.write_text(
            "ASCII render unavailable.\n"
            "Install optional dependency:\n"
            "  ./venv/bin/pip install grandalf\n",
            encoding="utf-8",
        )
    mermaid_path.write_text(mermaid_diagram, encoding="utf-8")

    png_path: Path | None = None
    png_error: str | None = None
    if export_png:
        try:
            png_bytes = graph_obj.draw_mermaid_png()
            png_path = out_dir / "langgraph_flow.png"
            png_path.write_bytes(png_bytes)
        except Exception as exc:
            png_error = str(exc)

    print(f"\n{sep}")
    print("  LangGraph Flow Export")
    print(f"  ASCII   : {ascii_path}")
    print(f"  Mermaid : {mermaid_path}")
    if ascii_error:
        print(f"  ASCII   : FAILED ({ascii_error})")
    if export_png and png_path:
        print(f"  PNG     : {png_path}")
    if export_png and png_error:
        print(f"  PNG     : FAILED ({png_error})")
    print(f"{sep}\n")

    if ascii_diagram is not None:
        print("[Graph ASCII]")
        print(ascii_diagram)
    else:
        print("[Graph ASCII] Unavailable. Install grandalf to enable terminal diagram.")

    return {
        "ascii_path": str(ascii_path),
        "mermaid_path": str(mermaid_path),
        "png_path": str(png_path) if png_path else None,
    }


def run_semantic_search(
    query: str,
    top_k: int = 5,
    top_k_chunks: int = 200,
    exclude_company: str = "",
    *,
    user_id: str,
) -> Dict[str, Any]:
    user_id = require_user_id(user_id, context="semantic search")
    sep = "=" * 70
    print(f"\n{sep}")
    print("  Semantic Company Search")
    print(f"  Query        : {query}")
    print(f"  Top companies: {top_k}")
    print(f"  Top chunks   : {top_k_chunks}")
    print(f"{sep}\n")

    try:
        response = get_search_service().search_companies(
            query=query,
            top_k=max(1, top_k),
            top_k_chunks=max(top_k_chunks, top_k * 10),
            exclude_company=exclude_company or "",
            include_full_data=False,
            user_id=user_id,
        )
    except Exception as exc:
        print("[Search Error] Semantic search is unavailable.")
        print(f"  Reason: {exc}")
        return {"query": query, "results": [], "error": str(exc)}

    print("[Search Results]")
    if not response["results"]:
        print("  No matches found.")
    for idx, row in enumerate(response["results"], start=1):
        print(
            f"  {idx}. {row.get('company_name')}  "
            f"(score={row.get('score')}, matches={row.get('match_count')}, "
            f"category={row.get('category')})"
        )

    return response


def _print_summary(result: Dict[str, Any], company_name: str, title: str) -> None:
    sep = "=" * 70
    golden = result.get("golden_record", {})
    test_results = result.get("test_results", {})
    if isinstance(golden, dict):
        field_count = len(golden)
    elif isinstance(golden, list):
        field_count = len(golden)
    else:
        field_count = 0

    print(f"\n{sep}")
    print(f"  {title} - {company_name}")
    print(f"{sep}")
    print(f"  Golden record : {field_count} fields")
    print(f"  Saved to      : {result.get('golden_record_path', 'N/A')}")
    if result.get("chunk_record_path"):
        print(f"  Chunks        : {result.get('chunk_record_path')}")
    print(f"  Pytest report : {result.get('pytest_report_path', 'N/A')}")

    if test_results:
        icon = "PASS" if test_results.get("all_passed") else "FAIL"
        print(
            f"\n  [{icon}] Test Results : "
            f"{test_results.get('passed', 0)} passed / "
            f"{test_results.get('failed', 0)} failed / "
            f"{test_results.get('skipped', 0)} skipped "
            f"({test_results.get('duration_sec', 0)}s)"
        )
        if test_results.get("failed_parameter_ids"):
            print(f"  Failed IDs    : {test_results.get('failed_parameter_ids')}")
    print(f"{sep}\n")


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Company Intelligence runner")
    parser.add_argument("--mode", choices=["full", "regen", "graph", "search"], default="full")
    parser.add_argument("--company", default="IBM", help="Company name")
    parser.add_argument("--query", default="", help="Semantic search query (search mode)")
    parser.add_argument(
        "--user-id",
        default="",
        help="Authenticated user id for scoped full, regen, and search modes",
    )

    parser.add_argument(
        "--base-record",
        help="Path to an existing golden_record JSON to use as baseline in regen mode",
    )
    parser.add_argument(
        "--failed-ids",
        default="",
        help="Comma-separated failed parameter IDs for regen mode (e.g. 77,88)",
    )
    parser.add_argument(
        "--pytest-report",
        default="",
        help="Optional pytest report JSON; failed IDs will be read from failed_parameter_ids",
    )
    parser.add_argument(
        "--max-rounds",
        type=int,
        default=2,
        help="Maximum remediation rounds in regen mode",
    )
    parser.add_argument(
        "--skip-tests",
        action="store_true",
        help="In regen mode, skip Agent4 test execution after save",
    )
    parser.add_argument(
        "--graph-dir",
        default="output",
        help="Directory to write graph export files in graph mode",
    )
    parser.add_argument(
        "--graph-png",
        action="store_true",
        help="In graph mode, also export a PNG via Mermaid renderer",
    )
    parser.add_argument(
        "--top-k",
        type=int,
        default=5,
        help="Semantic search: number of companies to return",
    )
    parser.add_argument(
        "--top-k-chunks",
        type=int,
        default=200,
        help="Semantic search: number of chunk hits to aggregate",
    )
    parser.add_argument(
        "--exclude-company",
        default="",
        help="Optional company name to exclude from search results",
    )
    return parser


if __name__ == "__main__":
    args = _build_parser().parse_args()

    if args.mode == "full":
        run_full_pipeline(args.company, user_id=args.user_id)
    elif args.mode == "graph":
        show_graph_flow(args.graph_dir, args.graph_png)
    elif args.mode == "search":
        if not args.query.strip():
            raise SystemExit("--query is required in --mode search")
        run_semantic_search(
            query=args.query.strip(),
            top_k=max(1, args.top_k),
            top_k_chunks=max(20, args.top_k_chunks),
            exclude_company=args.exclude_company.strip(),
            user_id=args.user_id,
        )
    else:
        if not args.base_record:
            raise SystemExit("--base-record is required in --mode regen")

        failed_ids = _parse_failed_ids(args.failed_ids)
        if args.pytest_report:
            failed_ids = _load_failed_ids_from_report(args.pytest_report)
        failed_ids = _normalize_ids(failed_ids)

        if not failed_ids:
            raise SystemExit(
                "No failed IDs provided. Use --failed-ids or --pytest-report in regen mode."
            )

        run_regeneration_only(
            company_name=args.company,
            base_record_path=args.base_record,
            failed_ids=failed_ids,
            max_rounds=max(1, args.max_rounds),
            run_pytests=not args.skip_tests,
            user_id=args.user_id,
        )

#for testing regeneration loops on specific parameter IDs without running the full pipeline:
'''
./venv/bin/python main.py --mode regen --company IBM \
  --base-record output/ibm_golden_record_20260302_201907.json \
  --failed-ids 77,88


'''
