"""
Agent 2 — Consolidation
Receives multiple candidate payloads from Agent 1, picks the best value per
schema key, and emits the canonical 163-key flat golden record object.

Accepted input formats in `combined_raw`:
  - New: list of flat dicts tagged with `__source_llm__`
  - Legacy: list of row dicts with `ID` / `Research Output / Data` / `Source`

Output format in state:
  - `golden_record`: canonical flat dict (163 keys)
  - `golden_record_sources`: {flat_key: selected_source}

Retry logic: if filled rows < MIN_KEYS, retry up to MAX_RETRIES.
"""
import json
import os
import re
import time
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple

from core.llms import get_llm_consolidation
from core.prompts import build_consolidation_prompt, _FLAT_KEYS, _FLAT_KEY_TO_ID

def _env_bool(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _env_int(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None:
        return default
    try:
        return int(value.strip())
    except (TypeError, ValueError):
        return default


def _env_float(name: str, default: float) -> float:
    value = os.getenv(name)
    if value is None:
        return default
    try:
        return float(value.strip())
    except (TypeError, ValueError):
        return default


FAST_MODE = _env_bool("PIPELINE_FAST_MODE", False)
MAX_RETRIES = _env_int("AGENT2_MAX_RETRIES", 1 if FAST_MODE else 3)
MIN_KEYS = _env_int("AGENT2_MIN_KEYS", 100)
RETRY_BACKOFF_SEC = _env_float("AGENT2_RETRY_BACKOFF_SEC", 0.0 if FAST_MODE else 5.0)
CHUNK_SLEEP_SEC = _env_float("AGENT2_CHUNK_SLEEP_SEC", 0.0 if FAST_MODE else 3.0)
EMPTY_VALUES = {"not found", "n/a", "na", "unknown", "none", "null", "", "-"}
SUSPICIOUS_TOKENS = {
    "john smith", "jane doe", "test user",
    "sample user", "placeholder", "example name",
}

CONTACT_KEYS = {
    "primary_contact_name", "primary_contact_title",
    "primary_contact_email", "primary_contact_phone",
}

SOURCE_ALIASES = {
    "llm1": "LLaMA-3.3-70b",
    "llm2": "LLaMA-3.1-70b",
    "llm3": "LLaMA-3.1-8b",
    "targeted": "targeted",
}


# ── Value quality helpers ─────────────────────────────────────────────────────

def _norm(value: Any) -> str:
    return str(value).strip() if value is not None else ""


def _is_empty(value: Any) -> bool:
    return _norm(value).lower() in EMPTY_VALUES


def _is_placeholder(key: str, value: Any) -> bool:
    v = _norm(value).lower()
    if not v:
        return False
    if v in SUSPICIOUS_TOKENS:
        return True
    if "example.com" in v or "test@" in v or "sample@" in v:
        return True
    if key in CONTACT_KEYS:
        if "john smith" in v or "jane doe" in v:
            return True
        if "@" in v:
            local = v.split("@", 1)[0]
            if local in {"john.smith", "jane.doe", "test", "sample", "demo", "placeholder"}:
                return True
            if re.match(r"^(john|jane)[._-]?(smith|doe)$", local):
                return True
    return False


def _value_score(key: str, value: Any) -> int:
    """
    Deterministic quality score for a single value.
    Higher = better. Used by deterministic fallback selector.
    """
    v = _norm(value)
    if _is_empty(v):
        return 0
    if _is_placeholder(key, v):
        return -10
    score = 1000 + min(len(v), 300)
    if "@" in v:
        score += 30
    if ";" in v:
        score += 10
    if any(ch.isdigit() for ch in v):
        score += 5
    return score


def _canonical_source(value: Any) -> str:
    src = _norm(value)
    if not src:
        return ""
    return SOURCE_ALIASES.get(src.lower(), src)


def _best_candidate(key: str, candidates: List[Tuple[str, str]]) -> Tuple[str, str]:
    """
    Deterministic fallback: pick the highest-scoring value from a list of
    candidate (value, source) tuples for a single key.
    """
    if not candidates:
        return "Not Found", ""
    ranked = sorted(
        candidates,
        key=lambda item: (_value_score(key, item[0]), len(_norm(item[0]))),
        reverse=True,
    )
    best_value, best_source = ranked[0]
    return (_norm(best_value) or "Not Found"), _canonical_source(best_source)


# ── Flat dict helpers ─────────────────────────────────────────────────────────

def _strip_meta(flat: Dict[str, Any]) -> Dict[str, str]:
    """Remove __source_llm__ and other internal metadata keys."""
    return {k: _norm(v) for k, v in flat.items() if not k.startswith("__")}


def _count_filled(flat: Dict[str, str]) -> int:
    return sum(1 for v in flat.values() if not _is_empty(v))


def _chunk_flat(flat: Dict[str, str], start_id: int, end_id: int) -> Dict[str, str]:
    """Extract only keys belonging to IDs start_id..end_id from a flat dict."""
    return {
        k: v for k, v in flat.items()
        if k in _FLAT_KEY_TO_ID and start_id <= _FLAT_KEY_TO_ID[k] <= end_id
    }


def _looks_like_row(item: Any) -> bool:
    return isinstance(item, dict) and (
        "ID" in item or "Research Output / Data" in item or "Parameter" in item
    )


def _source_flats_from_combined_raw(combined_raw: List[Dict[str, Any]]) -> List[Tuple[str, Dict[str, str]]]:
    """
    Normalize `combined_raw` into [(source_label, flat_dict)].
    Accepts both:
      - New format: list of flat dicts tagged with __source_llm__
      - Legacy format: list of row dicts with ID/Research Output / Data/Source
    """
    if not combined_raw:
        return []

    if all(_looks_like_row(item) for item in combined_raw):
        by_source: Dict[str, Dict[str, str]] = {}
        for row in combined_raw:
            try:
                id_ = int(row.get("ID", 0))
            except (TypeError, ValueError):
                continue
            key = _FLAT_KEYS.get(id_)
            if not key:
                continue
            source = _canonical_source(row.get("Source", ""))
            value = _norm(row.get("Research Output / Data", ""))
            by_source.setdefault(source, {})
            if value:
                by_source[source][key] = value
        return [(source, flat) for source, flat in by_source.items() if flat]

    normalized: List[Tuple[str, Dict[str, str]]] = []
    for item in combined_raw:
        if not isinstance(item, dict):
            continue
        source = _canonical_source(item.get("__source_llm__", ""))
        flat = _strip_meta(item)
        if flat:
            normalized.append((source, flat))
    return normalized


def _coerce_llm_chunk_to_flat(payload: Any, chunk_start: int, chunk_end: int) -> Dict[str, str]:
    """
    Parse consolidation LLM chunk payload into {flat_key: value} for the
    requested ID range. Supports both dict and legacy row-list payloads.
    """
    result: Dict[str, str] = {}

    if isinstance(payload, dict):
        for key, value in payload.items():
            id_ = _FLAT_KEY_TO_ID.get(key)
            if id_ is None or not (chunk_start <= id_ <= chunk_end):
                continue
            result[key] = _norm(value)
        return result

    if isinstance(payload, list):
        for row in payload:
            if not isinstance(row, dict):
                continue
            try:
                id_ = int(row.get("ID", 0))
            except (TypeError, ValueError):
                continue
            if not (chunk_start <= id_ <= chunk_end):
                continue
            key = _FLAT_KEYS.get(id_)
            if not key:
                continue
            value = row.get("Research Output / Data")
            if value is None:
                value = row.get("Research Output")
            if value is None:
                value = row.get("Data")
            result[key] = _norm(value)
        return result

    return result


def _repair_object(text: str) -> str:
    last = text.rfind('",')
    if last != -1:
        text = text[:last + 1]
    if not text.strip().endswith("}"):
        text += "\n}"
    return text


def _strip_fences(text: str) -> str:
    text = text.strip()
    if text.startswith("```"):
        parts = text.split("```")
        text  = parts[1] if len(parts) > 1 else text
        if text.startswith("json"):
            text = text[4:]
    return text.strip().rstrip("```").strip()


# ── Baseline loader (flat format) ─────────────────────────────────────────────

def _load_seed_flat(path_value: Any) -> Optional[Dict[str, str]]:
    """
    Load a baseline golden record file.
    Handles both the new flat format { key: value }
    and the old array format [{ ID, Parameter, Research Output/Data }].
    """
    if not path_value:
        return None

    raw_path = str(path_value)
    primary  = Path(raw_path)
    search   = [primary]
    if not primary.is_absolute():
        search.append(Path(__file__).resolve().parent.parent / primary)

    existing: Optional[Path] = next((p for p in search if p.exists()), None)
    if existing is None:
        print(f"[Agent2] Baseline not found: {raw_path}")
        return None

    try:
        with open(existing, encoding="utf-8") as f:
            data = json.load(f)
    except Exception as exc:
        print(f"[Agent2] Failed loading baseline: {exc}")
        return None

    # New flat format
    if isinstance(data, dict):
        flat = {k: _norm(v) for k, v in data.items() if k in _FLAT_KEY_TO_ID}
        print(f"[Agent2] Loaded baseline flat dict: {len(flat)} keys from {existing}")
        return flat

    # Old array format — convert to flat
    if isinstance(data, list):
        flat = {}
        for row in data:
            try:
                id_ = int(row.get("ID", 0))
            except (TypeError, ValueError):
                continue
            key = _FLAT_KEYS.get(id_)
            if key:
                flat[key] = _norm(row.get("Research Output / Data", ""))
        print(f"[Agent2] Loaded baseline array -> flat: {len(flat)} keys from {existing}")
        return flat

    return None


# ── Guardrail: LLM-chosen value vs deterministic best ────────────────────────

def _select_best_candidate(
    key: str,
    candidates: List[Tuple[str, str]],
    llm_value: Optional[str],
) -> Tuple[str, str]:
    """
    Guardrail selector for a single key:
    - If LLM value is empty/placeholder and a real candidate exists → use deterministic best
    - If LLM value matches one of the candidates → accept it (it's a real value)
    - If LLM invented a value not in any candidate → use deterministic best
    """
    best_value, best_source = _best_candidate(key, candidates)

    if llm_value is None:
        return best_value, best_source

    llm_norm = _norm(llm_value)

    # LLM picked empty while real data exists — override
    if _is_empty(llm_norm) and not _is_empty(best_value):
        return best_value, best_source

    # LLM value matches one of the real candidates — trust it
    for candidate_value, candidate_source in candidates:
        candidate_norm = _norm(candidate_value)
        if llm_norm != candidate_norm:
            continue
        # But check it's not a placeholder
        if _is_placeholder(key, llm_norm):
            return best_value, best_source
        return llm_norm, _canonical_source(candidate_source)

    # LLM invented something not in candidates — use deterministic best
    return best_value, best_source


# ── Main consolidation node ───────────────────────────────────────────────────

def run_consolidation(state: Dict[str, Any]) -> Dict[str, Any]:
    retries  = state.get("retry_consolidation", 0)

    # combined_raw = list of flat dicts from Agent 1, each tagged __source_llm__
    combined_raw: List[Dict[str, Any]] = list(state.get("combined_raw", []))

    # Normalize combined_raw into (source, flat_dict) tuples.
    source_flat_entries: List[Tuple[str, Dict[str, str]]] = _source_flats_from_combined_raw(combined_raw)

    # Merge remediated keys from targeted retry on top of existing sources
    remediated: Dict[str, str] = state.get("failed_param_candidates", {})
    if remediated:
        print(f"[Agent2] Applying {len(remediated)} remediated keys from targeted retry")
        # Add remediated as an extra source flat
        source_flat_entries.append(
            (
                "targeted",
                {k: _norm(v) for k, v in remediated.items() if k in _FLAT_KEY_TO_ID},
            )
        )

    # If no data at all, try loading from baseline file
    if not source_flat_entries:
        baseline_path = state.get("base_record_path") or state.get("golden_record_path")
        seed = _load_seed_flat(baseline_path)
        if seed:
            source_flat_entries = [("baseline", seed)]

    print(f"\n[Agent2] Consolidation — attempt {retries + 1}/{MAX_RETRIES}")
    print(f"[Agent2] Input: {len(source_flat_entries)} source flat dicts")

    if not source_flat_entries:
        print("[Agent2] No data — skipping")
        return {
            "golden_record":          {},
            "golden_record_sources":  {},
            "retry_consolidation":    retries,
            "failed_param_candidates": {},
        }

    if retries > 0 and RETRY_BACKOFF_SEC > 0:
        wait = retries * RETRY_BACKOFF_SEC
        print(f"[Agent2] Waiting {wait}s before retry...")
        time.sleep(wait)

    # Build per-key candidate lists: { key -> [(value, source), ...] }
    key_candidates: Dict[str, List[Tuple[str, str]]] = {}
    for source, flat in source_flat_entries:
        for key, value in flat.items():
            if key not in _FLAT_KEY_TO_ID:
                continue
            key_candidates.setdefault(key, [])
            v = _norm(value)
            if v:
                key_candidates[key].append((v, source))

    llm     = get_llm_consolidation()
    golden: Dict[str, str] = {}
    golden_sources: Dict[str, str] = {}

    # Process in 2 chunks to stay within LLM token limits
    for chunk_start, chunk_end in [(1, 82), (83, 163)]:

        # Slice each source flat to only keys in this chunk
        chunk_sources = [_chunk_flat(f, chunk_start, chunk_end) for _, f in source_flat_entries]
        chunk_sources = [c for c in chunk_sources if c]   # drop empty

        if not chunk_sources:
            print(f"  [Agent2] IDs {chunk_start}-{chunk_end}: no data, skipping")
            continue

        chunk_key_count = sum(len(c) for c in chunk_sources)
        print(f"  [Agent2] IDs {chunk_start}-{chunk_end}: "
              f"{len(chunk_sources)} sources, {chunk_key_count} candidate values")

        try:
            prompt      = build_consolidation_prompt(chunk_sources)
            response    = llm.invoke(prompt)
            raw_content = _strip_fences(response.content)

            parsed = False
            for text in [raw_content, _repair_object(raw_content)]:
                try:
                    flat_result = json.loads(text)
                    chunk_llm = _coerce_llm_chunk_to_flat(flat_result, chunk_start, chunk_end)
                    if not chunk_llm:
                        continue

                    chunk_golden: Dict[str, str] = {}
                    chunk_source: Dict[str, str] = {}
                    for key, llm_value in chunk_llm.items():
                        # Guardrail: validate LLM choice against real candidates
                        candidates = key_candidates.get(key, [])
                        selected, selected_source = _select_best_candidate(
                            key,
                            candidates,
                            _norm(llm_value),
                        )
                        chunk_golden[key] = selected
                        chunk_source[key] = selected_source

                    # Fill any keys the LLM missed using deterministic fallback
                    for id_ in range(chunk_start, chunk_end + 1):
                        key = _FLAT_KEYS.get(id_)
                        if key and key not in chunk_golden:
                            candidates = key_candidates.get(key, [])
                            selected, selected_source = _best_candidate(key, candidates)
                            chunk_golden[key] = selected
                            chunk_source[key] = selected_source

                    golden.update(chunk_golden)
                    golden_sources.update(chunk_source)
                    filled = sum(1 for v in chunk_golden.values() if not _is_empty(v))
                    print(f"  [Agent2] IDs {chunk_start}-{chunk_end}: "
                          f"{len(chunk_golden)} keys, {filled} filled")
                    parsed = True
                    break

                except json.JSONDecodeError:
                    continue

            if not parsed:
                # Full fallback — deterministic best per key for this chunk
                print(f"  [Agent2] Parse failed — deterministic fallback for IDs {chunk_start}-{chunk_end}")
                for id_ in range(chunk_start, chunk_end + 1):
                    key = _FLAT_KEYS.get(id_)
                    if key:
                        selected, selected_source = _best_candidate(key, key_candidates.get(key, []))
                        golden[key] = selected
                        golden_sources[key] = selected_source

        except Exception as e:
            print(f"  [Agent2] LLM call failed: {e} — deterministic fallback")
            for id_ in range(chunk_start, chunk_end + 1):
                key = _FLAT_KEYS.get(id_)
                if key:
                    selected, selected_source = _best_candidate(key, key_candidates.get(key, []))
                    golden[key] = selected
                    golden_sources[key] = selected_source

        if CHUNK_SLEEP_SEC > 0:
            time.sleep(CHUNK_SLEEP_SEC)

    filled_total = _count_filled(golden)
    print(f"\n[Agent2] Golden record: {len(golden)} keys, {filled_total} filled")

    return {
        "golden_record":          golden,
        "golden_record_sources":  golden_sources,
        "retry_consolidation":    retries,
        "combined_raw":           combined_raw,
        "failed_param_candidates": {},
    }


def check_consolidation(state: Dict[str, Any]) -> str:
    golden  = state.get("golden_record", {})
    if isinstance(golden, dict):
        filled = _count_filled(golden)
    elif isinstance(golden, list):
        filled = sum(
            1
            for row in golden
            if isinstance(row, dict) and not _is_empty(row.get("Research Output / Data", ""))
        )
    else:
        filled = 0
    retries = state.get("retry_consolidation", 0)
    if filled < MIN_KEYS and retries < MAX_RETRIES:
        print(f"[Router] Consolidation -> RETRY ({filled} filled keys)")
        return "retry_consolidation"
    print(f"[Router] Consolidation -> PASS ({filled} filled keys)")
    return "save"


def inc_retry_consolidation(state: Dict[str, Any]) -> Dict[str, Any]:
    n = state.get("retry_consolidation", 0) + 1
    print(f"[Retry] consolidation counter -> {n}")
    return {"retry_consolidation": n}
