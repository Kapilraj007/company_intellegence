"""
GraphState — Pipeline State
3 LLM outputs from Agent 1, consolidated by Agent 2, validated + saved by Agent 3,
then tested by Agent 4 (pytest runner node).
"""
from typing import TypedDict, Dict, Any, List, Optional


class GraphState(TypedDict, total=False):
    # Input
    company_name: str

    # Agent 1 — 3 LLM research outputs
    llm1_output: List[Dict[str, Any]]   # LLaMA 3.3 70b primary
    llm2_output: List[Dict[str, Any]]   # LLaMA 3.1 70b secondary
    llm3_output: List[Dict[str, Any]]   # LLaMA 3.1 8b  tertiary

    # Retry counters per agent
    retry_llm1: int
    retry_llm2: int
    retry_llm3: int
    retry_consolidation: int
    pytest_retry_count: int

    # Agent 2 — consolidated golden record
    combined_raw: List[Dict[str, Any]]
    base_record_path: Optional[str]
    golden_record: List[Dict[str, Any]]
    failed_param_candidates: List[Dict[str, Any]]
    failed_parameter_ids: List[int]

    # Agent 3 — saved file paths (passed to Agent 4)
    golden_record_path:     Optional[str]
    validation_report_path: Optional[str]

    # Agent 4 — pytest results
    test_results: Optional[Dict[str, Any]]
    pytest_report_path: Optional[str]
