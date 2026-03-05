"""
Agent 2 — Consolidation
1 LLM receives up to 3 candidate rows per ID and picks the best one.
Processes in 2 chunks (IDs 1–82, 83–163) to avoid token limits.
Retry logic: if golden_record < MIN_ROWS, retry up to MAX_RETRIES.
"""
import json
import re
import time
from collections import defaultdict
from pathlib import Path
from typing import Dict, Any, List

from core.llms import get_llm_consolidation
from core.prompts import build_consolidation_prompt

MAX_RETRIES = 3
MIN_ROWS    = 100
EMPTY_VALUES = {"not found", "n/a", "na", "unknown", "none", "null", "", "-"}
SUSPICIOUS_TOKENS = {
    "john smith",
    "jane doe",
    "test user",
    "sample user",
    "placeholder",
    "example name",
}


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


def _normalized_data(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _is_missing_value(value: Any) -> bool:
    return _normalized_data(value).lower() in EMPTY_VALUES


def _looks_synthetic_placeholder(row: Dict[str, Any]) -> bool:
    data = _normalized_data(row.get("Research Output / Data")).lower()
    parameter = _normalized_data(row.get("Parameter")).lower()
    if not data:
        return False

    if data in SUSPICIOUS_TOKENS:
        return True
    if "example.com" in data or "test@" in data or "sample@" in data:
        return True

    # High-risk personal contact fields: reject obvious placeholder identities.
    if "primary contact" in parameter:
        if "john smith" in data or "jane doe" in data:
            return True
        if "@" in data:
            local = data.split("@", 1)[0]
            if local in {"john.smith", "jane.doe", "test", "sample", "demo", "placeholder"}:
                return True
            if re.match(r"^(john|jane)[._-]?(smith|doe)$", local):
                return True

    return False


def _candidate_score(row: Dict[str, Any]) -> int:
    """
    Deterministic quality score for selecting best candidate rows.
    Higher is better.
    """
    data = _normalized_data(row.get("Research Output / Data"))
    if _is_missing_value(data):
        return 0
    if _looks_synthetic_placeholder(row):
        # Lower than "Not Found" to avoid promoting obvious hallucinations.
        return -10

    score = 1000
    score += min(len(data), 300)
    if "@" in data:
        score += 30
    if ";" in data:
        score += 10
    if any(ch.isdigit() for ch in data):
        score += 5
    return score


def _best_candidate(candidates: List[Dict[str, Any]]) -> Dict[str, Any] | None:
    if not candidates:
        return None
    ranked = sorted(
        candidates,
        key=lambda row: (
            _candidate_score(row),
            len(_normalized_data(row.get("Research Output / Data"))),
        ),
        reverse=True,
    )
    return dict(ranked[0])


def _select_candidate(
    candidates: List[Dict[str, Any]],
    llm_row: Dict[str, Any] | None = None,
) -> Dict[str, Any] | None:
    """
    Guardrail selector:
    - Always return one of the original candidate rows (never synthetic row).
    - If LLM row matches a candidate exactly on data, keep that candidate.
    - If LLM picked missing data while non-missing exists, override to best candidate.
    """
    best = _best_candidate(candidates)
    if best is None:
        return None
    if llm_row is None:
        return best

    llm_data = _normalized_data(llm_row.get("Research Output / Data"))
    llm_missing = _is_missing_value(llm_data)
    best_score = _candidate_score(best)

    # If LLM choice maps to a real candidate, only keep it when it is not worse than best.
    for cand in candidates:
        if _normalized_data(cand.get("Research Output / Data")) != llm_data:
            continue
        cand_score = _candidate_score(cand)
        if llm_missing and best_score > 0:
            return best
        # Keep LLM-selected candidate only when it is not worse than best.
        if cand_score >= best_score:
            return dict(cand)
        return best

    # LLM returned non-candidate or low-quality value; trust deterministic best.
    return best


def _load_seed_rows(path_value: Any) -> List[Dict[str, Any]]:
    if not path_value:
        return []

    raw_path = str(path_value)
    primary = Path(raw_path)
    candidates = [primary]

    if not primary.is_absolute():
        project_root = Path(__file__).resolve().parent.parent
        candidates.append(project_root / primary)

    existing_path: Path | None = None
    for candidate in candidates:
        if candidate.exists():
            existing_path = candidate
            break

    if existing_path is None:
        print(f"[Agent2] Baseline path not found: {raw_path}")
        return []

    try:
        with open(existing_path, encoding="utf-8") as f:
            data = json.load(f)
    except Exception as exc:
        print(f"[Agent2] Failed loading baseline record from {existing_path}: {exc}")
        return []

    if not isinstance(data, list):
        print(f"[Agent2] Baseline JSON is not a list: {existing_path}")
        return []

    rows = [row for row in data if isinstance(row, dict)]
    print(f"[Agent2] Loaded {len(rows)} baseline rows from {existing_path}")
    return rows


def run_consolidation(state: Dict[str, Any]) -> Dict[str, Any]:
    retries      = state.get("retry_consolidation", 0)
    combined     = list(state.get("combined_raw", []))
    retry_rows   = state.get("failed_param_candidates", [])

    if not combined:
        baseline_path = state.get("base_record_path") or state.get("golden_record_path")
        if baseline_path:
            combined.extend(_load_seed_rows(baseline_path))

    if retry_rows:
        print(f"[Agent2] Applying {len(retry_rows)} targeted retry candidates")
        combined.extend(retry_rows)

    print(f"\n[Agent2] Consolidation — attempt {retries + 1}/{MAX_RETRIES}")
    print(f"[Agent2] Input rows: {len(combined)}")

    if not combined:
        print("[Agent2] ❌ No combined data — skipping")
        return {
            "golden_record": [],
            "retry_consolidation": retries,
            "failed_param_candidates": [],
        }

    if retries > 0:
        wait = retries * 5
        print(f"[Agent2] Waiting {wait}s before retry...")
        time.sleep(wait)

    # Group by ID — each ID should have up to 3 candidates (one per LLM)
    grouped: Dict[int, List] = defaultdict(list)
    for row in combined:
        id_ = row.get("ID")
        if id_ is not None:
            grouped[int(id_)].append(row)

    llm    = get_llm_consolidation()
    golden = []

    # Process in 2 chunks to stay within token limits
    for chunk_start, chunk_end in [(1, 82), (83, 163)]:
        chunk_candidates = []
        for id_ in range(chunk_start, chunk_end + 1):
            chunk_candidates.extend(grouped.get(id_, []))

        if not chunk_candidates:
            print(f"  [Agent2] IDs {chunk_start}–{chunk_end}: no data, skipping")
            continue

        unique_ids = len(set(r.get("ID") for r in chunk_candidates))
        print(f"  [Agent2] IDs {chunk_start}–{chunk_end}: {len(chunk_candidates)} candidates for {unique_ids} IDs")

        try:
            prompt   = build_consolidation_prompt(chunk_candidates)
            response = llm.invoke(prompt)
            content  = _strip_fences(response.content)

            # Try parse, then repair
            for text in [content, _repair(content)]:
                try:
                    chunk_golden = json.loads(text)
                    chosen_rows: List[Dict[str, Any]] = []
                    chosen_ids = set()
                    for llm_row in chunk_golden:
                        try:
                            row_id = int(llm_row.get("ID"))
                        except (TypeError, ValueError):
                            continue
                        if row_id < chunk_start or row_id > chunk_end:
                            continue
                        selected = _select_candidate(grouped.get(row_id, []), llm_row)
                        if selected is None or row_id in chosen_ids:
                            continue
                        chosen_rows.append(selected)
                        chosen_ids.add(row_id)

                    # Fill any IDs omitted by the consolidation model.
                    for row_id in range(chunk_start, chunk_end + 1):
                        if row_id in chosen_ids:
                            continue
                        selected = _select_candidate(grouped.get(row_id, []))
                        if selected is not None:
                            chosen_rows.append(selected)
                            chosen_ids.add(row_id)

                    golden.extend(chosen_rows)
                    print(
                        f"  [Agent2] IDs {chunk_start}–{chunk_end}: {len(chosen_rows)} golden rows"
                    )
                    break
                except json.JSONDecodeError:
                    continue
            else:
                # Both failed — fallback: select best available candidate per ID
                print(f"  [Agent2] Parse failed — using fallback for IDs {chunk_start}–{chunk_end}")
                for id_ in range(chunk_start, chunk_end + 1):
                    selected = _select_candidate(grouped.get(id_, []))
                    if selected is not None:
                        golden.append(selected)

        except Exception as e:
            print(f"  [Agent2] LLM call failed: {e} — using fallback")
            for id_ in range(chunk_start, chunk_end + 1):
                selected = _select_candidate(grouped.get(id_, []))
                if selected is not None:
                    golden.append(selected)

        time.sleep(3)

    # Sort + deduplicate
    golden.sort(key=lambda x: x.get("ID", 0))
    seen, deduped = set(), []
    for row in golden:
        id_ = row.get("ID")
        if id_ not in seen:
            seen.add(id_)
            deduped.append(row)

    print(f"\n[Agent2] ✅ Golden record: {len(deduped)} rows")
    return {
        "golden_record": deduped,
        "retry_consolidation": retries,
        "combined_raw": combined,
        "failed_param_candidates": [],
    }


def check_consolidation(state: Dict[str, Any]) -> str:
    rows    = len(state.get("golden_record", []))
    retries = state.get("retry_consolidation", 0)
    if rows < MIN_ROWS and retries < MAX_RETRIES:
        print(f"[Router] Consolidation → RETRY ({rows} rows)")
        return "retry_consolidation"
    print(f"[Router] Consolidation → PASS ({rows} rows)")
    return "save"


def inc_retry_consolidation(state: Dict[str, Any]) -> Dict[str, Any]:
    n = state.get("retry_consolidation", 0) + 1
    print(f"[Retry] consolidation counter → {n}")
    return {"retry_consolidation": n}
