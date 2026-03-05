"""
Prompts — research prompt split into 2 chunks (IDs 1-82, 83-163)
to avoid token truncation. Consolidation prompt picks best row per ID.
"""
import json

# ── Full schema definition ────────────────────────────────────────────────────
_SCHEMA_ROWS = [
    (1,   "Company Basics",          "Company Name",                     "Atomic"),
    (2,   "Company Basics",          "Short Name",                       "Atomic"),
    (3,   "Company Basics",          "Logo URL",                         "Composite"),
    (4,   "Company Basics",          "Category",                         "Atomic"),
    (5,   "Company Basics",          "Year of Incorporation",            "Atomic"),
    (6,   "Company Narrative",       "Overview of the Company",          "Atomic"),
    (7,   "Company Basics",          "Nature of Company",                "Atomic"),
    (8,   "Company Basics",          "Company Headquarters",             "Atomic"),
    (9,   "Geographic Presence",     "Countries Operating In",           "Composite"),
    (10,  "Geographic Presence",     "Number of Offices",                "Atomic"),
    (11,  "Geographic Presence",     "Office Locations",                 "Composite"),
    (12,  "People & Talent",         "Employee Size",                    "Atomic"),
    (13,  "People & Talent",         "Hiring Velocity",                  "Composite"),
    (14,  "People & Talent",         "Employee Turnover",                "Atomic"),
    (15,  "People & Talent",         "Average Retention Tenure",         "Atomic"),
    (16,  "Business Model",          "Pain Points Being Addressed",      "Composite"),
    (17,  "Business Model",          "Focus Sectors / Industries",       "Composite"),
    (18,  "Business Model",          "Services / Offerings / Products",  "Composite"),
    (19,  "Business Model",          "Top Customers by Client Segments", "Composite"),
    (20,  "Business Model",          "Core Value Proposition",           "Composite"),
    (21,  "Strategy & Culture",      "Vision",                           "Atomic"),
    (22,  "Strategy & Culture",      "Mission",                          "Atomic"),
    (23,  "Strategy & Culture",      "Values",                           "Composite"),
    (24,  "Strategy & Culture",      "Unique Differentiators",           "Composite"),
    (25,  "Strategy & Culture",      "Competitive Advantages",           "Composite"),
    (26,  "Strategy & Culture",      "Weaknesses / Gaps in Offering",    "Composite"),
    (27,  "Strategy & Culture",      "Key Challenges and Unmet Needs",   "Composite"),
    (28,  "Competitive Landscape",   "Key Competitors",                  "Composite"),
    (29,  "Competitive Landscape",   "Technology Partners",              "Composite"),
    (30,  "Company Narrative",       "Interesting Facts",                "Composite"),
    (31,  "Company Narrative",       "Recent News",                      "Composite"),
    (32,  "Digital Presence",        "Website URL",                      "Atomic"),
    (33,  "Digital Presence",        "Quality of Website",               "Atomic"),
    (34,  "Digital Presence",        "Website Rating",                   "Atomic"),
    (35,  "Digital Presence",        "Website Traffic Rank",             "Composite"),
    (36,  "Digital Presence",        "Social Media Followers Combined",  "Atomic"),
    (37,  "Digital Presence",        "Glassdoor Rating",                 "Atomic"),
    (38,  "Digital Presence",        "Indeed Rating",                    "Atomic"),
    (39,  "Digital Presence",        "Google Reviews Rating",            "Atomic"),
    (40,  "Digital Presence",        "LinkedIn Profile URL",             "Atomic"),
    (41,  "Digital Presence",        "Twitter X Handle",                 "Atomic"),
    (42,  "Digital Presence",        "Facebook Page URL",                "Atomic"),
    (43,  "Digital Presence",        "Instagram Page URL",               "Atomic"),
    (44,  "Leadership",              "CEO Name",                         "Atomic"),
    (45,  "Leadership",              "CEO LinkedIn URL",                 "Atomic"),
    (46,  "Leadership",              "Key Business Leaders",             "Composite"),
    (47,  "Leadership",              "Warm Introduction Pathways",       "Composite"),
    (48,  "Leadership",              "Decision Maker Accessibility",     "Atomic"),
    (49,  "Contact Info",            "Company Contact Email",            "Atomic"),
    (50,  "Contact Info",            "Company Phone Number",             "Atomic"),
    (51,  "Contact Info",            "Primary Contact Name",             "Atomic"),
    (52,  "Contact Info",            "Primary Contact Title",            "Atomic"),
    (53,  "Contact Info",            "Primary Contact Email",            "Atomic"),
    (54,  "Contact Info",            "Primary Contact Phone",            "Atomic"),
    (55,  "Reputation",              "Awards and Recognitions",          "Composite"),
    (56,  "Reputation",              "Brand Sentiment Score",            "Atomic"),
    (57,  "Reputation",              "Event Participation",              "Composite"),
    (58,  "Risk & Compliance",       "Regulatory Compliance Status",     "Composite"),
    (59,  "Risk & Compliance",       "Legal Issues Controversies",       "Atomic"),
    (60,  "Financials",              "Annual Revenues",                  "Atomic"),
    (61,  "Financials",              "Annual Profits",                   "Atomic"),
    (62,  "Financials",              "Revenue Mix",                      "Composite"),
    (63,  "Financials",              "Company Valuation",                "Atomic"),
    (64,  "Financials",              "Year-over-Year Growth Rate",       "Atomic"),
    (65,  "Financials",              "Profitability Status",             "Atomic"),
    (66,  "Financials",              "Market Share",                     "Atomic"),
    (67,  "Funding",                 "Key Investors Backers",            "Composite"),
    (68,  "Funding",                 "Recent Funding Rounds",            "Composite"),
    (69,  "Funding",                 "Total Capital Raised",             "Atomic"),
    (70,  "Sustainability",          "ESG Practices or Ratings",         "Composite"),
    (71,  "Sales & Growth",          "Sales Motion",                     "Atomic"),
    (72,  "Sales & Growth",          "Customer Acquisition Cost",        "Atomic"),
    (73,  "Sales & Growth",          "Customer Lifetime Value",          "Atomic"),
    (74,  "Sales & Growth",          "CAC LTV Ratio",                    "Atomic"),
    (75,  "Sales & Growth",          "Churn Rate",                       "Atomic"),
    (76,  "Sales & Growth",          "Net Promoter Score",               "Atomic"),
    (77,  "Sales & Growth",          "Customer Concentration Risk",      "Atomic"),
    (78,  "Sales & Growth",          "Burn Rate",                        "Atomic"),
    (79,  "Sales & Growth",          "Runway",                           "Atomic"),
    (80,  "Sales & Growth",          "Burn Multiplier",                  "Atomic"),
    (81,  "Innovation",              "Intellectual Property",            "Composite"),
    (82,  "Innovation",              "R&D Investment",                   "Atomic"),
    (83,  "Innovation",              "AI ML Adoption Level",             "Atomic"),
    (84,  "Operations",              "Tech Stack Tools Used",            "Composite"),
    (85,  "Operations",              "Cybersecurity Posture",            "Composite"),
    (86,  "Operations",              "Supply Chain Dependencies",        "Composite"),
    (87,  "Operations",              "Geopolitical Risks",               "Composite"),
    (88,  "Operations",              "Macro Risks",                      "Composite"),
    (89,  "People & Talent",         "Diversity Metrics",                "Composite"),
    (90,  "People & Talent",         "Remote Work Policy",               "Atomic"),
    (91,  "People & Talent",         "Training Development Spend",       "Atomic"),
    (92,  "Market",                  "Partnership Ecosystem",            "Composite"),
    (93,  "Market",                  "Exit Strategy History",            "Composite"),
    (94,  "Sustainability",          "Carbon Footprint",                 "Atomic"),
    (95,  "Sustainability",          "Ethical Sourcing Practices",       "Composite"),
    (96,  "Benchmarking",            "Benchmark vs Peers",               "Composite"),
    (97,  "Forecasting",             "Future Projections",               "Atomic"),
    (98,  "Forecasting",             "Strategic Priorities",             "Composite"),
    (99,  "Network",                 "Industry Associations",            "Composite"),
    (100, "Proof Points",            "Case Studies",                     "Composite"),
    (101, "Go-to-Market",            "Go-to-Market Strategy",            "Composite"),
    (102, "Innovation",              "Innovation Roadmap",               "Composite"),
    (103, "Innovation",              "Product Pipeline",                 "Composite"),
    (104, "Governance",              "Board of Directors Advisors",      "Composite"),
    (105, "Digital Presence",        "Company Marketing Videos",         "Composite"),
    (106, "Proof Points",            "Customer Testimonials",            "Composite"),
    (107, "Benchmarking",            "Industry Benchmark Tech Adoption", "Composite"),
    (108, "Market",                  "Total Addressable Market TAM",     "Atomic"),
    (109, "Market",                  "Serviceable Addressable Market",   "Atomic"),
    (110, "Market",                  "Serviceable Obtainable Market",    "Atomic"),
    (111, "Culture & People",        "Work Culture",                     "Composite"),
    (112, "Culture & People",        "Manager Quality",                  "Atomic"),
    (113, "Culture & People",        "Psychological Safety",             "Atomic"),
    (114, "Culture & People",        "Feedback Culture",                 "Composite"),
    (115, "Culture & People",        "Diversity Inclusion",              "Composite"),
    (116, "Culture & People",        "Ethical Standards",                "Composite"),
    (117, "Work-Life Balance",       "Typical Working Hours",            "Atomic"),
    (118, "Work-Life Balance",       "Overtime Expectations",            "Atomic"),
    (119, "Work-Life Balance",       "Weekend Work",                     "Atomic"),
    (120, "Work-Life Balance",       "Remote Hybrid Flexibility",        "Composite"),
    (121, "Work-Life Balance",       "Leave Policy",                     "Composite"),
    (122, "Work-Life Balance",       "Burnout Risk",                     "Atomic"),
    (123, "Location & Accessibility","Central vs Peripheral Location",   "Atomic"),
    (124, "Location & Accessibility","Public Transport Access",          "Composite"),
    (125, "Location & Accessibility","Cab Availability Policy",          "Composite"),
    (126, "Location & Accessibility","Commute Time from Airport",        "Atomic"),
    (127, "Location & Accessibility","Office Zone Type",                 "Atomic"),
    (128, "Safety & Well-being",     "Area Safety",                      "Composite"),
    (129, "Safety & Well-being",     "Company Safety Policies",          "Composite"),
    (130, "Safety & Well-being",     "Office Infrastructure Safety",     "Composite"),
    (131, "Safety & Well-being",     "Emergency Response Preparedness",  "Composite"),
    (132, "Safety & Well-being",     "Health Support",                   "Composite"),
    (133, "Learning & Growth",       "Onboarding Training Quality",      "Atomic"),
    (134, "Learning & Growth",       "Learning Culture",                 "Composite"),
    (135, "Learning & Growth",       "Exposure Quality",                 "Atomic"),
    (136, "Learning & Growth",       "Mentorship Availability",          "Composite"),
    (137, "Learning & Growth",       "Internal Mobility",                "Atomic"),
    (138, "Learning & Growth",       "Promotion Clarity",                "Composite"),
    (139, "Learning & Growth",       "Tools Technology Access",          "Composite"),
    (140, "Role & Work Quality",     "Role Clarity",                     "Atomic"),
    (141, "Role & Work Quality",     "Early Ownership",                  "Atomic"),
    (142, "Role & Work Quality",     "Work Impact",                      "Composite"),
    (143, "Role & Work Quality",     "Execution vs Thinking Balance",    "Atomic"),
    (144, "Role & Work Quality",     "Automation Level",                 "Atomic"),
    (145, "Role & Work Quality",     "Cross-functional Exposure",        "Composite"),
    (146, "Company Stability",       "Company Maturity",                 "Atomic"),
    (147, "Company Stability",       "Brand Value",                      "Atomic"),
    (148, "Company Stability",       "Client Quality",                   "Composite"),
    (149, "Company Stability",       "Layoff History",                   "Atomic"),
    (150, "Compensation & Benefits", "Fixed vs Variable Pay",            "Atomic"),
    (151, "Compensation & Benefits", "Bonus Predictability",             "Atomic"),
    (152, "Compensation & Benefits", "ESOPs Long-term Incentives",       "Composite"),
    (153, "Compensation & Benefits", "Family Health Insurance",          "Composite"),
    (154, "Compensation & Benefits", "Relocation Support",               "Composite"),
    (155, "Compensation & Benefits", "Lifestyle Wellness Benefits",      "Composite"),
    (156, "Long-Term Career",        "Exit Opportunities",               "Composite"),
    (157, "Long-Term Career",        "Skill Relevance",                  "Atomic"),
    (158, "Long-Term Career",        "External Recognition",             "Atomic"),
    (159, "Long-Term Career",        "Network Strength",                 "Composite"),
    (160, "Long-Term Career",        "Global Exposure",                  "Composite"),
    (161, "Values Alignment",        "Mission Clarity",                  "Atomic"),
    (162, "Values Alignment",        "Sustainability and CSR",           "Composite"),
    (163, "Values Alignment",        "Crisis Behavior",                  "Atomic"),
]

_INSTRUCTIONS = """RULES:
- Return ONLY a valid JSON array. No markdown, no explanation, no extra text.
- Each object must have EXACTLY these 5 keys:
    "ID"                    → the integer ID number from the schema
    "Category"              → the category string from the schema (copy exactly)
    "A/C"                   → MUST be exactly the word "Atomic" or the word "Composite" (copy from schema column A/C)
    "Parameter"             → the parameter name from the schema (copy exactly)
    "Research Output / Data"→ your researched answer for this parameter
- CRITICAL: "A/C" must ONLY contain "Atomic" or "Composite". Never put company name or any other value there.
- Atomic field → single value string in "Research Output / Data".
- Composite field → multiple values separated by semicolons e.g. "Val1;Val2;Val3" in "Research Output / Data".
- Never leave blank. Use "Not Found" only if truly unavailable.
- Output exactly {count} objects — one per ID listed."""

_SCHEMA_BY_ID = {row[0]: row for row in _SCHEMA_ROWS}


def _build_schema_table(rows: list) -> str:
    lines = ["ID | Category | Parameter | A/C"]
    for id_, cat, param, ac in rows:
        lines.append(f"{id_} | {cat} | {param} | {ac}")
    return "\n".join(lines)


def build_research_prompt(company_name: str, chunk: int) -> str:
    """
    chunk=1 → IDs 1–82
    chunk=2 → IDs 83–163
    Splitting avoids token truncation at ID 70-80.
    """
    if chunk == 1:
        rows = [r for r in _SCHEMA_ROWS if r[0] <= 82]
    else:
        rows = [r for r in _SCHEMA_ROWS if r[0] > 82]

    count      = len(rows)
    start_id   = rows[0][0]
    end_id     = rows[-1][0]
    schema     = _build_schema_table(rows)
    instruct   = _INSTRUCTIONS.replace("{count}", str(count))

    return f"""You are a corporate research analyst. Research "{company_name}" and fill in data for IDs {start_id}–{end_id}.

{instruct}

SCHEMA:
{schema}

Return the JSON array now."""


def build_targeted_research_prompt(company_name: str, field_ids: list[int]) -> str:
    """
    Prompt for remediation loop:
    return only the requested IDs that failed pytest-linked checks.
    """
    ids_set = set()
    for raw in field_ids:
        try:
            value = int(raw)
        except (TypeError, ValueError):
            continue
        if value in _SCHEMA_BY_ID:
            ids_set.add(value)
    ids = sorted(ids_set)
    if not ids:
        raise ValueError("No valid field IDs provided for targeted research prompt")

    rows = [_SCHEMA_BY_ID[i] for i in ids]
    count = len(rows)
    schema = _build_schema_table(rows)
    instruct = _INSTRUCTIONS.replace("{count}", str(count))
    id_list = ", ".join(str(i) for i in ids)

    return f"""You are a corporate research analyst. Research "{company_name}" and fill ONLY these IDs: {id_list}.

{instruct}
- IMPORTANT: Do NOT output any ID outside this list: {id_list}.

SCHEMA:
{schema}

Return the JSON array now."""


def build_consolidation_prompt(candidates: list) -> str:
    """
    Agent 2 prompt — receives 3 candidate rows per ID (one per LLM).
    Selects the single best row per ID.
    """
    count = len(set(r.get("ID") for r in candidates))

    return f"""You are a data quality engine. You have {len(candidates)} candidate rows for {count} unique IDs.
Each ID has up to 3 candidate rows from different LLMs. Pick the single best row per ID.

PRIORITY:
Reliability > Completeness. A truthful "Not Found" is better than a fabricated value.

RULES:
- Return ONLY a valid JSON array. No markdown, no explanation.
- Output exactly {count} objects — one per unique ID.
- Never invent, infer, merge, or modify values. Pick one existing candidate row exactly as-is.
- Keep the Source field from the selected candidate row.
- Sort output by ID ascending.

SELECTION POLICY:
- Prefer candidates that are specific and consistent across multiple sources.
- If candidates conflict and confidence is low, choose the conservative value ("Not Found"/"N/A"/"Unknown").
- Treat obvious placeholders as low-quality (examples: "John Smith", "Jane Doe", "test@example.com", generic demo values).
- For high-risk personal contact fields (IDs 51-54), do NOT prefer a single-source, placeholder-like identity over "Not Found".
- For speculative metrics (for example burn-related fields), avoid precise-looking guesses when candidates disagree.

CANDIDATES:
{json.dumps(candidates, indent=2)}

Return the consolidated JSON array now."""
