import json
from core.llms import get_llm
from core.prompts import RESEARCH_PROMPT
from core.state import GraphState


# -------- LLM CALL WRAPPER -------- #

def call_llm(provider: str, company_name: str):
    llm = get_llm(provider)

    response = llm.invoke(
        RESEARCH_PROMPT.format(company_name=company_name)
    )

    try:
        return json.loads(response.content)
    except Exception:
        return []


# -------- RESEARCH NODES -------- #

def research_groq(state: GraphState):
    company = state["company_name"]
    data = call_llm("groq", company)
    return {"groq_output": data}


def research_openrouter(state: GraphState):
    company = state["company_name"]
    data = call_llm("openrouter", company)
    return {"openrouter_output": data}


def research_gemini(state: GraphState):
    company = state["company_name"]
    data = call_llm("gemini", company)
    return {"gemini_output": data}


# -------- COMBINE NODE -------- #

def combine_results(state: GraphState):
    combined = (
        state.get("groq_output", []) +
        state.get("openrouter_output", []) +
        state.get("gemini_output", [])
    )

    return {"combined_output": combined}