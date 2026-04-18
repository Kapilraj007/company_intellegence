"""
GraphState — Pipeline State
3 LLM outputs from Agent 1, consolidated by Agent 2, validated + saved by Agent 3,
then tested by Agent 4 (pytest runner node).
"""
from typing import TypedDict, Dict, Any, List, Optional


class GraphState(TypedDict, total=False):
    # Input
    company_name: str
    user_id:      str

    run_id:        str          # ← ADD
    company_id:    str          # ← ADD
    # Agent 1 — 3 LLM research outputs (flat dict per LLM, 163 keys each)
    llm1_output: Dict[str, str]   # LLaMA 3.3 70b primary   — { "company_name": "...", ... }
    llm2_output: Dict[str, str]   # LLaMA 3.1 70b secondary
    llm3_output: Dict[str, str]   # LLaMA 3.1 8b  tertiary

    # Retry counters per agent
    retry_llm1: int
    retry_llm2: int
    retry_llm3: int
    retry_consolidation: int
    pytest_retry_count: int

    # Agent 2 — consolidated golden record (single flat object, 163 keys)
    combined_raw: List[Dict[str, Any]]   # list of 3 flat dicts, each tagged __source_llm__
    base_record_path: Optional[str]
    golden_record: Dict[str, str]
    golden_record_sources: Dict[str, str]
    failed_param_candidates: Dict[str, str]   # flat dict of remediated field key→value
    failed_parameter_ids: List[int]
    semantic_chunks: List[Dict[str, Any]]
    chunk_coverage: Dict[str, Any]

    # Agent 3 — saved file paths (passed to Agent 4)
    golden_record_path:     Optional[str]
    validation_report_path: Optional[str]
    flat_record_path:       Optional[str]   # alias of golden_record_path for compatibility
    chunk_record_path:      Optional[str]

    # Agent 4 — pytest results
    test_results: Optional[Dict[str, Any]]
    pytest_report_path: Optional[str]
