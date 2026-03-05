"""
LLM clients — Groq only (free tier)
Agent 1: 3 different LLMs for diverse research
Agent 2: 1 strong LLM for consolidation
"""
import os
from langchain_groq import ChatGroq


# ── Agent 1 — 3 research LLMs ────────────────────────────────────────────────

def get_llm_primary():
    """LLaMA 3.3 70b — strongest, most reliable JSON."""
    return ChatGroq(
        model="llama-3.3-70b-versatile",
        temperature=0.2,
        groq_api_key=os.getenv("GROQ_API_KEY"),
        max_tokens=8000,
    )


def get_llm_secondary():
    """LLaMA 3.3 70b (temp 0.4) — second diverse source."""
    return ChatGroq(
        model="llama-3.3-70b-versatile",
        temperature=0.4,
        groq_api_key=os.getenv("GROQ_API_KEY"),
        max_tokens=8000,
    )


def get_llm_tertiary():
    """LLaMA 3.1 8b — fast lightweight third source."""
    return ChatGroq(
        model="llama-3.1-8b-instant",
        temperature=0.2,
        groq_api_key=os.getenv("GROQ_API_KEY"),
        max_tokens=8000,
    )


# ── Agent 2 — 1 consolidation LLM ────────────────────────────────────────────

def get_llm_consolidation():
    """LLaMA 3.3 70b — best model for picking the golden row."""
    return ChatGroq(
        model="llama-3.3-70b-versatile",
        temperature=0.1,
        groq_api_key=os.getenv("GROQ_API_KEY"),
        max_tokens=8000,
    )