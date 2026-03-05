"""
LangGraph Pipeline
==================
Flow:
  llm1 → [retry?] → llm2 → [retry?] → llm3 → [retry?]
    → combine → consolidate → [retry?] → save → run_tests
    → [pytest fail on specific IDs?] → targeted_research → consolidate → ... → END

Agent 1: 3 LLMs research 163 fields each
Agent 2: 1 LLM consolidates into golden record
Agent 3: Pydantic validates + saves JSON (returns file paths into state)
Agent 4: pytest runner — tests sections 12.5, 13.2, 13.3, 13.4, 14.1, 14.2
"""
from langgraph.graph import StateGraph, END

from core.state import GraphState
from agents.agent1_research import (
    run_llm1, run_llm2, run_llm3, combine_outputs, run_targeted_research,
    check_llm1, check_llm2, check_llm3,
    inc_retry_llm1, inc_retry_llm2, inc_retry_llm3,
)
from agents.agent2_consolidation import (
    run_consolidation, check_consolidation, inc_retry_consolidation,
)
from agents.agent3_save        import save_output
from agents.agent4_test_runner import run_tests, route_after_tests

builder = StateGraph(GraphState)

# ── Agent 1 nodes ──────────────────────────────────────────────────────────────
builder.add_node("llm1",              run_llm1)
builder.add_node("llm2",              run_llm2)
builder.add_node("llm3",              run_llm3)
builder.add_node("combine",           combine_outputs)
builder.add_node("retry_inc_llm1",    inc_retry_llm1)
builder.add_node("retry_inc_llm2",    inc_retry_llm2)
builder.add_node("retry_inc_llm3",    inc_retry_llm3)
builder.add_node("llm1_failed_param_retry", run_targeted_research)

# ── Agent 2 nodes ──────────────────────────────────────────────────────────────
builder.add_node("consolidate",           run_consolidation)
builder.add_node("retry_inc_consolidate", inc_retry_consolidation)

# ── Agent 3 node ───────────────────────────────────────────────────────────────
builder.add_node("save",       save_output)

# ── Agent 4 node ───────────────────────────────────────────────────────────────
builder.add_node("run_tests",  run_tests)

# ── Entry ──────────────────────────────────────────────────────────────────────
builder.set_entry_point("llm1")

# ── LLM1 retry ────────────────────────────────────────────────────────────────
builder.add_conditional_edges("llm1", check_llm1, {
    "retry_llm1": "retry_inc_llm1",
    "pass":        "llm2",
})
builder.add_edge("retry_inc_llm1", "llm1")

# ── LLM2 retry ────────────────────────────────────────────────────────────────
builder.add_conditional_edges("llm2", check_llm2, {
    "retry_llm2": "retry_inc_llm2",
    "pass":        "llm3",
})
builder.add_edge("retry_inc_llm2", "llm2")

# ── LLM3 retry ────────────────────────────────────────────────────────────────
builder.add_conditional_edges("llm3", check_llm3, {
    "retry_llm3": "retry_inc_llm3",
    "pass":        "combine",
})
builder.add_edge("retry_inc_llm3", "llm3")

# ── Combine → Consolidate ─────────────────────────────────────────────────────
builder.add_edge("combine", "consolidate")

# ── Consolidation retry ───────────────────────────────────────────────────────
builder.add_conditional_edges("consolidate", check_consolidation, {
    "retry_consolidation": "retry_inc_consolidate",
    "save":                 "save",
})
builder.add_edge("retry_inc_consolidate", "consolidate")

# ── Save → Run Tests → END ────────────────────────────────────────────────────
builder.add_edge("save",       "run_tests")
builder.add_conditional_edges("run_tests", route_after_tests, {
    "retry_via_llm1": "llm1_failed_param_retry",
    "end": END,
})
builder.add_edge("llm1_failed_param_retry", "consolidate")

graph = builder.compile()
