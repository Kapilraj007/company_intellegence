"""
Agent 1 — Research
3 LLMs each research all 163 fields in 2 chunks (IDs 1–82, 83–163).

OUTPUT FORMAT CHANGE:
  Before: list of 163 row-dicts per LLM  →  489 rows stored in DB
  Now:    one flat dict per LLM           →  3 rows stored in DB
          { "company_name": "Stripe", "ceo_name": "...", ... }

State keys:
  llm1_output  →  Dict[str, str]   (flat object, 163 keys)
  llm2_output  →  Dict[str, str]
  llm3_output  →  Dict[str, str]
  combined_raw →  List[Dict]  — 3 flat objects, each tagged with __source_llm__
"""
import json
import os
import threading
import time
from datetime import datetime, timezone
from typing import Dict, Any, List

from core.llms import get_llm_primary, get_llm_secondary, get_llm_tertiary
from core.prompts import (
    build_research_prompt,
    build_targeted_research_prompt,
    _FLAT_KEY_TO_ID,
    _FLAT_KEYS,
)

from core.local_store import get_local_store_client
from core.supabase_store import get_supabase_client
from core.user_scope import require_user_id
from logger import get_logger

logger = get_logger("agent1_research")

MAX_RETRIES = 3
MIN_KEYS    = 80   # minimum filled keys to consider an LLM output acceptable
# Agent1 writes raw outputs to local JSON storage.


def _env_float(name: str, default: float) -> float:
    value = os.getenv(name)
    if value is None:
        return default
    try:
        return float(value.strip())
    except (TypeError, ValueError):
        return default


# AGENT1 vector upsert removed — persistence is local-file based.


# ── Helpers ───────────────────────────────────────────────────────────────────

def _require_company_name(state: Dict[str, Any], node_name: str) -> str:
    company = state.get("company_name")
    if not isinstance(company, str) or not company.strip():
        raise ValueError(
            f"{node_name} requires state['company_name'] (non-empty string)."
        )
    return company.strip()


def _require_state_user_id(state: Dict[str, Any], node_name: str) -> str:
    return require_user_id(state.get("user_id"), context=f"Agent1 {node_name}")


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


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _slug(value: str) -> str:
    return "".join(ch if ch.isalnum() else "_" for ch in value.lower()).strip("_") or "unknown"


def _get_ids(state: Dict[str, Any]) -> tuple[str, str, str]:
    company_name = str(state.get("company_name", "unknown") or "unknown")
    run_id = str(state.get("run_id") or f"local-{_slug(company_name)}")
    company_id = str(state.get("company_id") or _slug(company_name))
    user_id = require_user_id(state.get("user_id"), context="Agent1 state")
    return run_id, company_id, user_id


def _strip_fences(text: str) -> str:
    text = text.strip()
    if text.startswith("```"):
        parts = text.split("```")
        text  = parts[1] if len(parts) > 1 else text
        if text.startswith("json"):
            text = text[4:]
    return text.strip().rstrip("```").strip()


def _repair_object(text: str) -> str:
    """Try to close a truncated JSON object."""
    last = text.rfind('",')
    if last != -1:
        text = text[:last + 1]
    if not text.strip().endswith("}"):
        text += "\n}"
    return text


def _parse_flat(text: str, source: str) -> Dict[str, str]:
    """
    Parse LLM response into a clean flat dict { snake_key: value }.
    Only keeps keys that exist in _FLAT_KEY_TO_ID (valid 163 schema keys).
    Falls back to _repair_object if JSON is truncated.
    Handles old array format as graceful fallback.
    Returns empty dict on total failure.
    """
    text = _strip_fences(text)
    for attempt, t in enumerate([text, _repair_object(text)]):
        try:
            data = json.loads(t)

            # ── Expected: flat object ─────────────────────────────────────────
            if isinstance(data, dict):
                clean: Dict[str, str] = {}
                for key, value in data.items():
                    if key in _FLAT_KEY_TO_ID:
                        clean[key] = str(value).strip() or "Not Found"
                if attempt > 0:
                    print(f"    [{source}] JSON repaired — {len(clean)} keys")
                return clean

            # ── Fallback: old array format → convert to flat ──────────────────
            if isinstance(data, list):
                print(f"    [{source}] Warning: got array, converting to flat object")
                flat: Dict[str, str] = {}
                for row in data:
                    try:
                        id_ = int(row.get("ID", 0))
                    except (TypeError, ValueError):
                        continue
                    key = _FLAT_KEYS.get(id_)
                    if key:
                        flat[key] = str(row.get("Research Output / Data", "Not Found")).strip()
                return flat

        except json.JSONDecodeError:
            continue

    print(f"    [{source}] JSON parse failed completely")
    return {}


def _count_filled(flat: Dict[str, str]) -> int:
    """Count keys with real data (not empty / Not Found / Unknown)."""
    empty = {"not found", "n/a", "unknown", "none", "null", ""}
    return sum(1 for v in flat.values() if v.lower() not in empty)


# Agent1 does not use vector DB writes. Raw data is persisted locally as JSON.


# ── Run one LLM across 2 chunks → single merged flat dict ────────────────────

def _run_llm(llm_factory, company: str, source: str, attempt: int) -> Dict[str, str]:
    """
    Runs chunk 1 (IDs 1-82) then chunk 2 (IDs 83-163).
    Merges both responses into ONE flat dict with up to 163 keys.
    """
    if attempt > 0:
        wait = attempt * 5
        print(f"  [{source}] Retry {attempt}/{MAX_RETRIES} — waiting {wait}s")
        time.sleep(wait)

    llm    = llm_factory()
    merged: Dict[str, str] = {}

    for chunk in [1, 2]:
        ids_label = "1-82" if chunk == 1 else "83-163"
        print(f"  [{source}] Chunk {chunk} (IDs {ids_label})...")
        try:
            prompt   = build_research_prompt(company, chunk)
            response = llm.invoke(prompt)
            flat     = _parse_flat(response.content, source)
            merged.update(flat)
            print(f"  [{source}] Chunk {chunk} -> {len(flat)} keys")
        except Exception as e:
            print(f"  [{source}] Chunk {chunk} failed: {e}")
        time.sleep(3)

    return merged


def _run_llm_for_ids(
    llm_factory,
    company: str,
    source: str,
    field_ids: List[int],
    attempt: int,
) -> Dict[str, str]:
    """
    Targeted retry: returns flat dict for only the requested field IDs.
    """
    if attempt > 0:
        wait = attempt * 5
        print(f"  [{source}] Retry {attempt} — waiting {wait}s")
        time.sleep(wait)

    llm = llm_factory()
    try:
        prompt   = build_targeted_research_prompt(company, sorted(field_ids))
        response = llm.invoke(prompt)
        flat     = _parse_flat(response.content, source)
    except Exception as e:
        print(f"  [{source}] Targeted call failed: {e}")
        return {}

    # Keep only keys that were actually requested
    wanted_keys = {_FLAT_KEYS[i] for i in field_ids if i in _FLAT_KEYS}
    result      = {k: v for k, v in flat.items() if k in wanted_keys}
    missing     = wanted_keys - set(result.keys())

    print(f"  [{source}] Targeted -> {len(result)}/{len(wanted_keys)} keys filled")
    if missing:
        print(f"  [{source}] Missing: {sorted(missing)}")

    time.sleep(2)
    return result


# ── LangGraph node functions ──────────────────────────────────────────────────

def run_llm1(state: Dict[str, Any]) -> Dict[str, Any]:
    attempt = state.get("retry_llm1", 0)
    company = _require_company_name(state, "llm1")
    user_id = _require_state_user_id(state, "llm1")
    print(f"\n[LLM1 - LLaMA 3.3 70b] attempt {attempt + 1}")

    flat   = _run_llm(get_llm_primary, company, "LLaMA-3.3-70b", attempt)
    filled = _count_filled(flat)
    print(f"[LLM1] {len(flat)} keys total, {filled} filled")

    run_id, company_id, user_id = _get_ids(state)
    try:
        get_supabase_client().insert_agent1_output(
            run_id=run_id,
            company_id=company_id,
            company_name=state["company_name"],
            source_llm="llm1",
            raw_data=dict(flat),
            filled_count=filled,
            user_id=user_id,
        )
    except Exception as e:
        logger.warning(f"Supabase insert failed (non-fatal): {e}")

    return {"llm1_output": flat, "retry_llm1": attempt}


def run_llm2(state: Dict[str, Any]) -> Dict[str, Any]:
    attempt = state.get("retry_llm2", 0)
    company = _require_company_name(state, "llm2")
    user_id = _require_state_user_id(state, "llm2")
    print(f"\n[LLM2 - LLaMA 3.1 70b] attempt {attempt + 1}")
    time.sleep(5)

    flat   = _run_llm(get_llm_secondary, company, "LLaMA-3.1-70b", attempt)
    filled = _count_filled(flat)
    print(f"[LLM2] {len(flat)} keys total, {filled} filled")

    run_id, company_id, user_id = _get_ids(state)
    try:
        get_supabase_client().insert_agent1_output(
            run_id=run_id,
            company_id=company_id,
            company_name=state["company_name"],
            source_llm="llm2",
            raw_data=dict(flat),
            filled_count=filled,
            user_id=user_id,
        )
    except Exception as e:
        logger.warning(f"Supabase insert failed (non-fatal): {e}")

    return {"llm2_output": flat, "retry_llm2": attempt}


def run_llm3(state: Dict[str, Any]) -> Dict[str, Any]:
    attempt = state.get("retry_llm3", 0)
    company = _require_company_name(state, "llm3")
    user_id = _require_state_user_id(state, "llm3")
    print(f"\n[LLM3 - LLaMA 3.1 8b] attempt {attempt + 1}")
    time.sleep(5)

    flat   = _run_llm(get_llm_tertiary, company, "LLaMA-3.1-8b", attempt)
    filled = _count_filled(flat)
    print(f"[LLM3] {len(flat)} keys total, {filled} filled")

    run_id, company_id, user_id = _get_ids(state)
    try:
        get_supabase_client().insert_agent1_output(
            run_id=run_id,
            company_id=company_id,
            company_name=state["company_name"],
            source_llm="llm3",
            raw_data=dict(flat),
            filled_count=filled,
            user_id=user_id,
        )
    except Exception as e:
        logger.warning(f"Supabase insert failed (non-fatal): {e}")

    return {"llm3_output": flat, "retry_llm3": attempt}


def combine_outputs(state: Dict[str, Any]) -> Dict[str, Any]:
    """
    Packages all 3 LLM flat dicts into a list for Agent 2.
    Each dict is tagged with __source_llm__ so Agent 2 can trace
    which LLM produced which value. Agent 2 strips this before saving.
    """
    llm1 = state.get("llm1_output", {})
    llm2 = state.get("llm2_output", {})
    llm3 = state.get("llm3_output", {})
    user_id = _require_state_user_id(state, "combine")

    combined = []
    for flat, source in [(llm1, "llm1"), (llm2, "llm2"), (llm3, "llm3")]:
        if flat:
            entry = dict(flat)
            entry["__source_llm__"] = source
            combined.append(entry)

    print(f"\n[Combine] llm1={len(llm1)} + llm2={len(llm2)} + llm3={len(llm3)} keys")
    print(f"[Combine] Passing {len(combined)} flat objects to Agent 2")

    # Store the complete Agent-1 raw bundle (3x163 = 489 raw attributes).
    run_id, company_id, user_id = _get_ids(state)
    try:
        store = get_local_store_client()
        store.insert_company_raw_data(
            run_id=run_id,
            company_name=state["company_name"],
            company_id=company_id,
            user_id=user_id,
            raw_json={
                "llm1": {k: v for k, v in llm1.items() if not k.startswith("__")},
                "llm2": {k: v for k, v in llm2.items() if not k.startswith("__")},
                "llm3": {k: v for k, v in llm3.items() if not k.startswith("__")},
                "generated_at": _utc_now(),
                "schema_field_count": 163,
                "raw_attribute_count": len(llm1) + len(llm2) + len(llm3),
            },
        )
    except Exception as e:
        logger.warning(f"Local store company_raw_data insert failed (non-fatal): {e}")

    logger.info("Agent1 complete — raw data stored in local JSON store.")

    return {"combined_raw": combined}


def run_targeted_research(state: Dict[str, Any]) -> Dict[str, Any]:
    """
    Retry node after pytest failures.
    Fetches only the specific failed field IDs across all 3 LLMs.
    LLM1 (strongest) runs last so it wins on key conflicts.
    """
    test_results = state.get("test_results", {})
    failed_ids   = _normalize_field_ids(test_results.get("failed_parameter_ids", []))
    retry_round  = state.get("pytest_retry_count", 0) + 1
    company      = _require_company_name(state, "llm1_failed_param_retry")
    user_id      = _require_state_user_id(state, "targeted_research")

    if not failed_ids:
        print("\n[Agent1-Targeted] No failed parameter IDs — skipping.")
        return {
            "failed_param_candidates": {},
            "failed_parameter_ids":    [],
            "pytest_retry_count":      retry_round,
        }

    print(f"\n[Agent1-Targeted] Retry round {retry_round} for IDs: {failed_ids}")

    # LLM3 first (weakest), LLM1 last (strongest) — last write wins on conflicts
    remediated: Dict[str, str] = {}
    for factory, source in [
        (get_llm_tertiary,  "LLaMA-3.1-8b"),
        (get_llm_secondary, "LLaMA-3.1-70b"),
        (get_llm_primary,   "LLaMA-3.3-70b"),
    ]:
        patch = _run_llm_for_ids(factory, company, source, failed_ids, retry_round - 1)
        remediated.update(patch)

    print(f"[Agent1-Targeted] Collected {len(remediated)} remediated keys")

    run_id, company_id, user_id = _get_ids(state)
    try:
        store = get_local_store_client()
        store.insert_agent1_flat(
            run_id=run_id,
            company_name=state["company_name"],
            company_id=company_id,
            source_llm="targeted",
            full_json=remediated,
            user_id=user_id,
        )
    except Exception as e:
        logger.warning(f"Local store update failed (non-fatal): {e}")

    logger.info("Agent1 targeted research complete — local JSON store updated.")

    return {
        "failed_param_candidates": remediated,
        "failed_parameter_ids":    failed_ids,
        "pytest_retry_count":      retry_round,
    }


# ── Retry routers ─────────────────────────────────────────────────────────────

def check_llm1(state: Dict[str, Any]) -> str:
    filled  = _count_filled(state.get("llm1_output", {}))
    retries = state.get("retry_llm1", 0)
    if filled < MIN_KEYS and retries < MAX_RETRIES:
        print(f"[Router] LLM1 -> RETRY ({filled} filled keys)")
        return "retry_llm1"
    print(f"[Router] LLM1 -> PASS ({filled} filled keys)")
    return "pass"


def check_llm2(state: Dict[str, Any]) -> str:
    filled  = _count_filled(state.get("llm2_output", {}))
    retries = state.get("retry_llm2", 0)
    if filled < MIN_KEYS and retries < MAX_RETRIES:
        print(f"[Router] LLM2 -> RETRY ({filled} filled keys)")
        return "retry_llm2"
    print(f"[Router] LLM2 -> PASS ({filled} filled keys)")
    return "pass"


def check_llm3(state: Dict[str, Any]) -> str:
    filled  = _count_filled(state.get("llm3_output", {}))
    retries = state.get("retry_llm3", 0)
    if filled < MIN_KEYS and retries < MAX_RETRIES:
        print(f"[Router] LLM3 -> RETRY ({filled} filled keys)")
        return "retry_llm3"
    print(f"[Router] LLM3 -> PASS ({filled} filled keys)")
    return "pass"


# ── Retry increment nodes ─────────────────────────────────────────────────────

def inc_retry_llm1(state: Dict[str, Any]) -> Dict[str, Any]:
    n = state.get("retry_llm1", 0) + 1
    print(f"[Retry] LLM1 counter -> {n}")
    return {"retry_llm1": n}


def inc_retry_llm2(state: Dict[str, Any]) -> Dict[str, Any]:
    n = state.get("retry_llm2", 0) + 1
    print(f"[Retry] LLM2 counter -> {n}")
    return {"retry_llm2": n}


def inc_retry_llm3(state: Dict[str, Any]) -> Dict[str, Any]:
    n = state.get("retry_llm3", 0) + 1
    print(f"[Retry] LLM3 counter -> {n}")
    return {"retry_llm3": n}
