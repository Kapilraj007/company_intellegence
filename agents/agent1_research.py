"""
Agent 1 — Research
3 LLMs each research all 163 fields in 2 chunks (IDs 1–82, 83–163).
Retry logic per LLM: if rows < MIN_ROWS, retry up to MAX_RETRIES times.
"""
import json
import time
from typing import Dict, Any, List

from core.llms import get_llm_primary, get_llm_secondary, get_llm_tertiary
from core.prompts import build_research_prompt, build_targeted_research_prompt

MAX_RETRIES = 3
MIN_ROWS    = 80


def _require_company_name(state: Dict[str, Any], node_name: str) -> str:
    company = state.get("company_name")
    if not isinstance(company, str) or not company.strip():
        raise ValueError(
            f"{node_name} requires state['company_name'] (non-empty string)."
        )
    return company.strip()


def _normalize_field_ids(values: List[Any]) -> List[int]:
    ids = set()
    for raw in values:
        try:
            value = int(raw)
        except (TypeError, ValueError):
            continue
        if 1 <= value <= 163:
            ids.add(value)
    return sorted(ids)


def _strip_fences(text: str) -> str:
    text = text.strip()
    if text.startswith("```"):
        parts = text.split("```")
        text  = parts[1] if len(parts) > 1 else text
        if text.startswith("json"):
            text = text[4:]
    return text.strip().rstrip("```").strip()


def _repair(text: str) -> str:
    last = text.rfind("},")
    if last == -1:
        last = text.rfind("}")
    if last != -1:
        text = text[:last + 1]
    if not text.strip().endswith("]"):
        text += "\n]"
    return text


def _parse(text: str, source: str) -> List[Dict[str, Any]]:
    text = _strip_fences(text)
    for attempt, t in enumerate([text, _repair(text)]):
        try:
            data = json.loads(t)
            for row in data:
                row["Source"] = source
            if attempt > 0:
                print(f"    [{source}] JSON repaired — {len(data)} rows")
            return data
        except json.JSONDecodeError:
            continue
    print(f"    [{source}] ⚠️  JSON parse failed")
    return []


# ── Run one LLM across 2 chunks ───────────────────────────────────────────────

def _run_llm(llm_factory, company: str, source: str, attempt: int) -> List[Dict[str, Any]]:
    if attempt > 0:
        wait = attempt * 5
        print(f"  [{source}] 🔄 Retry {attempt}/{MAX_RETRIES} — waiting {wait}s")
        time.sleep(wait)

    llm      = llm_factory()
    all_rows = []

    for chunk in [1, 2]:
        ids = "1–82" if chunk == 1 else "83–163"
        print(f"  [{source}] Chunk {chunk} (IDs {ids})...")
        try:
            prompt   = build_research_prompt(company, chunk)
            response = llm.invoke(prompt)
            rows     = _parse(response.content, source)
            all_rows.extend(rows)
            print(f"  [{source}] Chunk {chunk} → {len(rows)} rows")
        except Exception as e:
            print(f"  [{source}] Chunk {chunk} failed: {e}")
        time.sleep(3)   # rate limit buffer between chunks

    return all_rows


def _run_llm_for_ids(
    llm_factory,
    company: str,
    source: str,
    field_ids: List[int],
    attempt: int,
) -> List[Dict[str, Any]]:
    if attempt > 0:
        wait = attempt * 5
        print(f"  [{source}] 🔄 Retry {attempt} — waiting {wait}s")
        time.sleep(wait)

    wanted_ids = {int(i) for i in field_ids}
    llm = llm_factory()

    try:
        prompt = build_targeted_research_prompt(company, sorted(wanted_ids))
        response = llm.invoke(prompt)
        rows = _parse(response.content, source)
    except Exception as e:
        print(f"  [{source}] Targeted call failed: {e}")
        return []

    filtered: List[Dict[str, Any]] = []
    seen = set()
    for row in rows:
        try:
            row_id = int(row.get("ID"))
        except (TypeError, ValueError):
            continue
        if row_id in wanted_ids and row_id not in seen:
            filtered.append(row)
            seen.add(row_id)

    missing = sorted(wanted_ids - seen)
    print(f"  [{source}] Targeted IDs → {len(filtered)}/{len(wanted_ids)} rows")
    if missing:
        print(f"  [{source}] Missing IDs in response: {missing}")
    time.sleep(2)
    return filtered


# ── LangGraph node functions ──────────────────────────────────────────────────

def run_llm1(state: Dict[str, Any]) -> Dict[str, Any]:
    attempt = state.get("retry_llm1", 0)
    company = _require_company_name(state, "llm1")
    print(f"\n[LLM1 — LLaMA 3.3 70b] attempt {attempt + 1}")
    rows = _run_llm(get_llm_primary, company, "LLaMA-3.3-70b", attempt)
    print(f"[LLM1] Total: {len(rows)} rows")
    return {"llm1_output": rows, "retry_llm1": attempt}


def run_llm2(state: Dict[str, Any]) -> Dict[str, Any]:
    attempt = state.get("retry_llm2", 0)
    company = _require_company_name(state, "llm2")
    print(f"\n[LLM2 — LLaMA 3.1 70b] attempt {attempt + 1}")
    time.sleep(5)   # stagger to avoid rate limits
    rows = _run_llm(get_llm_secondary, company, "LLaMA-3.1-70b", attempt)
    print(f"[LLM2] Total: {len(rows)} rows")
    return {"llm2_output": rows, "retry_llm2": attempt}


def run_llm3(state: Dict[str, Any]) -> Dict[str, Any]:
    attempt = state.get("retry_llm3", 0)
    company = _require_company_name(state, "llm3")
    print(f"\n[LLM3 — LLaMA 3.1 8b] attempt {attempt + 1}")
    time.sleep(5)
    rows = _run_llm(get_llm_tertiary, company, "LLaMA-3.1-8b", attempt)
    print(f"[LLM3] Total: {len(rows)} rows")
    return {"llm3_output": rows, "retry_llm3": attempt}


def combine_outputs(state: Dict[str, Any]) -> Dict[str, Any]:
    combined = (
        state.get("llm1_output", []) +
        state.get("llm2_output", []) +
        state.get("llm3_output", [])
    )
    print(f"\n[Combine] {len(state.get('llm1_output', []))} + "
          f"{len(state.get('llm2_output', []))} + "
          f"{len(state.get('llm3_output', []))} = {len(combined)} rows")
    return {"combined_raw": combined}


def run_targeted_research(state: Dict[str, Any]) -> Dict[str, Any]:
    """
    Retry node after pytest failures:
    reruns Agent 1 only for failed parameter IDs extracted by Agent 4.
    """
    test_results = state.get("test_results", {})
    failed_ids = _normalize_field_ids(test_results.get("failed_parameter_ids", []))
    retry_round = state.get("pytest_retry_count", 0) + 1
    company = _require_company_name(state, "llm1_failed_param_retry")

    if not failed_ids:
        print("\n[Agent1-Targeted] No failed parameter IDs provided — skipping.")
        return {
            "failed_param_candidates": [],
            "failed_parameter_ids": [],
            "pytest_retry_count": retry_round,
        }

    print(f"\n[Agent1-Targeted] Retry round {retry_round} for IDs: {failed_ids}")

    candidates = []
    candidates.extend(
        _run_llm_for_ids(
            get_llm_primary,
            company,
            "LLaMA-3.3-70b",
            failed_ids,
            retry_round - 1,
        )
    )
    candidates.extend(
        _run_llm_for_ids(
            get_llm_secondary,
            company,
            "LLaMA-3.1-70b",
            failed_ids,
            retry_round - 1,
        )
    )
    candidates.extend(
        _run_llm_for_ids(
            get_llm_tertiary,
            company,
            "LLaMA-3.1-8b",
            failed_ids,
            retry_round - 1,
        )
    )

    print(f"[Agent1-Targeted] Collected {len(candidates)} candidate rows")
    return {
        "failed_param_candidates": candidates,
        "failed_parameter_ids": failed_ids,
        "pytest_retry_count": retry_round,
    }


# ── Retry routers ─────────────────────────────────────────────────────────────

def check_llm1(state: Dict[str, Any]) -> str:
    rows    = len(state.get("llm1_output", []))
    retries = state.get("retry_llm1", 0)
    if rows < MIN_ROWS and retries < MAX_RETRIES:
        print(f"[Router] LLM1 → RETRY ({rows} rows)")
        return "retry_llm1"
    print(f"[Router] LLM1 → PASS ({rows} rows)")
    return "pass"


def check_llm2(state: Dict[str, Any]) -> str:
    rows    = len(state.get("llm2_output", []))
    retries = state.get("retry_llm2", 0)
    if rows < MIN_ROWS and retries < MAX_RETRIES:
        print(f"[Router] LLM2 → RETRY ({rows} rows)")
        return "retry_llm2"
    print(f"[Router] LLM2 → PASS ({rows} rows)")
    return "pass"


def check_llm3(state: Dict[str, Any]) -> str:
    rows    = len(state.get("llm3_output", []))
    retries = state.get("retry_llm3", 0)
    if rows < MIN_ROWS and retries < MAX_RETRIES:
        print(f"[Router] LLM3 → RETRY ({rows} rows)")
        return "retry_llm3"
    print(f"[Router] LLM3 → PASS ({rows} rows)")
    return "pass"


# ── Retry increment nodes ─────────────────────────────────────────────────────

def inc_retry_llm1(state: Dict[str, Any]) -> Dict[str, Any]:
    n = state.get("retry_llm1", 0) + 1
    print(f"[Retry] LLM1 counter → {n}")
    return {"retry_llm1": n}


def inc_retry_llm2(state: Dict[str, Any]) -> Dict[str, Any]:
    n = state.get("retry_llm2", 0) + 1
    print(f"[Retry] LLM2 counter → {n}")
    return {"retry_llm2": n}


def inc_retry_llm3(state: Dict[str, Any]) -> Dict[str, Any]:
    n = state.get("retry_llm3", 0) + 1
    print(f"[Retry] LLM3 counter → {n}")
    return {"retry_llm3": n}
